package manager

import (
	"github.com/belyaevedu/philharmonic/task"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[string][]*task.Task
	EventDb       map[string][]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int
}

func (m *Manager) SelectWorker() string {
	// naive round-robin
	// e-pvm later

	var newWorker int
	if m.LastWorker+1 < len(m.Workers) {
		newWorker = m.LastWorker + 1
	} else {
		m.LastWorker = 0
	}
	m.LastWorker = newWorker

	return m.Workers[newWorker]
}

func (m *Manager) UpdateTasks() {
	// updates tasks
}

func (m *Manager) SendTask() {
	// sends task to worker(s)
}
