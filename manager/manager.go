package manager

import (
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"github.com/belyaevedu/philharmonic/task"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[string][]*task.Task
	EventDb       map[string][]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
}

func (m *Manager) SelectWorker() {
	// selects worker by reqs
}

func (m *Manager) UpdateTasks() {
	// updates tasks
}

func (m *Manager) SendTask() {
	// sends task to worker(s)
}
