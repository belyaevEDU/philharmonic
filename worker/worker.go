package worker

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
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
}

func (w *Worker) getTasks() []*task.Task {
	tasks := slices.Collect(maps.Values(w.Db))
	if tasks == nil {
		return []*task.Task{}
	}
	return tasks
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

// action is picked depending on the task's state
func (w *Worker) runTask() task.DockerResult {
	t := w.Queue.Dequeue()
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

	taskPersisted := w.Db[taskQueued.ID]
	if taskPersisted == nil {
		taskPersisted = &taskQueued
		w.Db[taskQueued.ID] = &taskQueued
	}

	var result task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
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
			taskPersisted.State, taskQueued.State, taskQueued.ID.String(),
		)
	}
	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()

	config := task.NewConfig(&t)

	d, err := task.NewDocker(config)
	if err != nil {
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
		w.Db[t.ID] = &t
		return result
	}

	t.ContainerID = result.ContainerID
	t.State = task.Running

	inspect := d.Inspect(result.ContainerID)
	if inspect.Error != nil {
		log.Printf("Error inspecting container %s after start: %v\n", result.ContainerID, inspect.Error)
	} else {
		t.HostPorts = task.PortMappingsFromPortMap(inspect.Response.NetworkSettings.Ports)
	}

	w.Db[t.ID] = &t

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
	t.State = task.Completed
	w.Db[t.ID] = &t

	log.Printf("Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)

	return result
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
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

func (w *Worker) updateTasks() {
	for id, t := range w.Db {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				fmt.Printf("error updating task: %v\n", resp.Error)
			}

			if resp.Response == nil {
				log.Printf("No container for running task %s\n", id)
				w.Db[id].State = task.Failed
			}

			if resp.Response.State.Status == container.StateExited {
				log.Printf(
					"Container for task %s in non-running state %s",
					id, resp.Response.State.Status,
				)
				w.Db[id].State = task.Failed
			}

			w.Db[id].HostPorts = task.PortMappingsFromPortMap(resp.Response.NetworkSettings.Ports)
		}
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
