package worker

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	Stats     *Stats
	TaskCount int

	dbMu    sync.RWMutex
	queueMu sync.Mutex
}

func (w *Worker) getTasks() []*task.Task {
	w.dbMu.RLock()
	defer w.dbMu.RUnlock()

	tasks := make([]*task.Task, 0, len(w.Db))
	for _, persisted := range w.Db {
		copy := *persisted
		tasks = append(tasks, &copy)
	}
	return tasks
}

func (w *Worker) getTask(id uuid.UUID) (task.Task, bool) {
	w.dbMu.RLock()
	defer w.dbMu.RUnlock()

	persisted, ok := w.Db[id]
	if !ok {
		return task.Task{}, false
	}
	return *persisted, true
}

func (w *Worker) setTask(t task.Task) {
	w.dbMu.Lock()
	if w.Db == nil {
		w.Db = make(map[uuid.UUID]*task.Task)
	}
	w.Db[t.ID] = &t
	w.dbMu.Unlock()
}

func (w *Worker) AddTask(t task.Task) {
	w.queueMu.Lock()
	w.Queue.Enqueue(t)
	w.queueMu.Unlock()
}

func (w *Worker) queueLen() int {
	w.queueMu.Lock()
	defer w.queueMu.Unlock()
	return w.Queue.Len()
}

func (w *Worker) dequeueTask() any {
	w.queueMu.Lock()
	defer w.queueMu.Unlock()
	return w.Queue.Dequeue()
}

// action is picked depending on the task's state
func (w *Worker) runTask() task.DockerResult {
	t := w.dequeueTask()
	if t == nil {
		log.Println("No tasks in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued, ok := t.(task.Task)
	if !ok {
		return task.DockerResult{
			Error: errors.New(
				"error pulling a task off the queue: somehow there's a non-task.Task element",
			),
		}
	}

	w.dbMu.Lock()
	taskPersisted := w.Db[taskQueued.ID]
	if taskPersisted == nil {
		persisted := taskQueued
		taskPersisted = &persisted
		w.Db[taskQueued.ID] = taskPersisted
	}
	persistedState := taskPersisted.State
	w.dbMu.Unlock()

	var result task.DockerResult
	if task.ValidStateTransition(persistedState, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(taskQueued)
		case task.Completed:
			result = w.StopTask(taskQueued)
		default:
			result.Error = fmt.Errorf(
				"error when trying to run task %s: state machine failed on state %v",
				taskQueued.ID.String(), taskQueued.State,
			)
		}
	} else {
		result.Error = fmt.Errorf(
			"invalid transition from %v to %v for task %s",
			persistedState, taskQueued.State, taskQueued.ID.String(),
		)
	}
	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()

	config := task.NewConfig(&t)

	d, err := task.NewDocker(config)
	if err != nil {
		t.State = task.Failed
		t.FailureReason = fmt.Sprintf("could not create docker client: %v", err)
		w.setTask(t)
		return task.DockerResult{Error: err}
	}

	if t.ContainerID != "" {
		log.Printf("Removing container %s of task %s before (re)starting it\n", t.ContainerID, t.ID)
		d.Stop(t.ContainerID)
	}

	result := d.Run()
	if result.Error != nil {
		log.Printf("Error running task %v: %v\n", t.ID, result.Error)
		t.State = task.Failed
		t.FailureReason = fmt.Sprintf("container failed to start: %v", result.Error)
		w.setTask(t)
		return result
	}

	t.ContainerID = result.ContainerID
	t.State = task.Running

	inspect := d.Inspect(result.ContainerID)
	if inspect.Error != nil {
		log.Printf("Error inspecting container %s after start: %v\n", result.ContainerID, inspect.Error)
	} else if inspect.Response == nil || inspect.Response.NetworkSettings == nil {
		log.Printf("Container %s returned no network settings after start\n", result.ContainerID)
	} else {
		t.HostPorts = task.PortMappingsFromPortMap(inspect.Response.NetworkSettings.Ports)
	}

	w.setTask(t)

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)

	d, err := task.NewDocker(config)
	if err != nil {
		return task.DockerResult{Error: err}
	}

	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("Error stopping container %v: %v\n", t.ContainerID, result.Error)
	}

	t.FinishTime = time.Now().UTC()

	persisted, exists := w.getTask(t.ID)
	if !exists {
		msg := fmt.Sprintf("error: task with ID %s isn't in the worker db", t.ID.String())
		fmt.Println(msg)
		return task.DockerResult{Error: errors.New(msg)}
	}

	switch {
	case t.FailureReason != "":
		t.State = task.Failed
	case persisted.State == task.Failed: // worker ahead of mngr fallback
		t.State = task.Failed
		t.FailureReason = persisted.FailureReason
	default:
		t.State = task.Completed
	}

	w.setTask(t)

	log.Printf("Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)

	return result
}

func (w *Worker) RunTasks() {
	for {
		if w.queueLen() != 0 {
			result := w.runTask()
			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Println("No tasks to process currently.")
		}
		time.Sleep(time.Second * 10)
	}
}

func (w *Worker) CollectStats() {
	for {
		log.Println("Collecting stats...")
		w.Stats = GetStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d, err := task.NewDocker(config)
	if err != nil {
		return task.DockerInspectResponse{Error: err}
	}
	return d.Inspect(t.ContainerID)
}

func healthCheckFailureReason(health *container.Health) string {
	if health != nil && len(health.Log) > 0 {
		last := health.Log[len(health.Log)-1]
		if last != nil {
			output := strings.TrimSpace(last.Output)
			if output != "" {
				return fmt.Sprintf("docker healthcheck failed with exit code %d: %s", last.ExitCode, output)
			}

			return fmt.Sprintf("docker healthcheck failed with exit code %d", last.ExitCode)
		}
	}

	if health != nil {
		return fmt.Sprintf(
			"container reported unhealthy by the docker health check (failing streak: %d)",
			health.FailingStreak,
		)
	}

	return "container reported unhealthy by the docker health check"
}

func (w *Worker) updateTasks() {
	w.dbMu.RLock()
	runningTasks := make([]task.Task, 0, len(w.Db))
	for _, persisted := range w.Db {
		if persisted.State == task.Running {
			runningTasks = append(runningTasks, *persisted)
		}
	}
	w.dbMu.RUnlock()

	for _, runningTask := range runningTasks {
		resp := w.InspectTask(runningTask)
		if resp.Error != nil {
			log.Printf("error updating task %s: %v\n", runningTask.ID, resp.Error)
		}

		w.dbMu.Lock()
		persisted, ok := w.Db[runningTask.ID]
		if !ok || persisted.State != task.Running {
			w.dbMu.Unlock()
			continue
		}

		if resp.Response == nil {
			persisted.State = task.Failed
			if resp.Error != nil {
				persisted.FailureReason = fmt.Sprintf("container inspection failed: %v", resp.Error)
			} else {
				persisted.FailureReason = "no container found for running task"
			}
			w.dbMu.Unlock()
			continue
		}

		if resp.Response.State == nil {
			persisted.State = task.Failed
			persisted.FailureReason = "container inspection returned no state"
			w.dbMu.Unlock()
			continue
		}

		if resp.Response.State.Status == container.StateExited {
			log.Printf(
				"Container for task %s in non-running state %s",
				runningTask.ID, resp.Response.State.Status,
			)
			persisted.State = task.Failed
			persisted.FailureReason = fmt.Sprintf("container exited with code %d", resp.Response.State.ExitCode)
		} else if resp.Response.State.Health != nil && resp.Response.State.Health.Status == container.Unhealthy {
			log.Printf("Container for task %s is unhealthy", runningTask.ID)
			persisted.State = task.Failed
			persisted.FailureReason = healthCheckFailureReason(resp.Response.State.Health)
		}

		if resp.Response.NetworkSettings != nil {
			persisted.HostPorts = task.PortMappingsFromPortMap(resp.Response.NetworkSettings.Ports)
		}
		w.dbMu.Unlock()
	}
}

func (w *Worker) UpdateTasks() {
	for {
		log.Println("Checking status of tasks")
		w.updateTasks()
		log.Println("Task updates completed")
		log.Println("Sleeping for 10 seconds...")
		time.Sleep(10 * time.Second)
	}
}
