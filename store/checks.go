package store

import "github.com/belyaevedu/philharmonic/task"

var (
	_ Store[task.Task]      = (*BoltStore[task.Task])(nil)
	_ Store[task.TaskEvent] = (*BoltStore[task.TaskEvent])(nil)
	_ Store[task.Task]      = (*InMemoryStore[task.Task])(nil)
	_ Store[task.TaskEvent] = (*InMemoryStore[task.TaskEvent])(nil)
)
