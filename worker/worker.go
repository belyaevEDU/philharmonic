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
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
}

func (w *Worker) CollectStats() {
	// collects stats
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
func (w *Worker) RunTask() task.DockerResult {
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

	result := d.Run()
	if result.Error != nil {
		log.Printf("Error running task %v: %v\n", t.ID, result.Error)
		t.State = task.Failed
		w.Db[t.ID] = &t
		return result
	}

	t.ContainerID = result.ContainerID
	t.State = task.Running
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
