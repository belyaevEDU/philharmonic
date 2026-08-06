package worker

import (
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"github.com/belyaevedu/philharmonic/task"
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

func (w *Worker) RunTask() {
	// start/stops task
}

func (w *Worker) StartTask() {
	// starts task
}

func (w *Worker) StopTask() {
	// stops task
}
