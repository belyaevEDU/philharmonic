package store

import (
	"fmt"
	"sync"

	"github.com/belyaevedu/philharmonic/task"
)

type InMemoryTaskStore struct {
	mu sync.RWMutex
	db map[string]*task.Task
}

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		db: make(map[string]*task.Task),
	}
}

func (i *InMemoryTaskStore) Put(key string, value any) error {
	t, ok := value.(*task.Task)
	if !ok || t == nil {
		return fmt.Errorf("value %v is not a task.Task type", value)
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if i.db == nil {
		i.db = make(map[string]*task.Task)
	}
	i.db[key] = t
	return nil
}

func (i *InMemoryTaskStore) Get(key string) (any, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	t, ok := i.db[key]
	if !ok {
		return nil, fmt.Errorf("task with key %s does not exist", key)
	}

	return t, nil
}

func (i *InMemoryTaskStore) List() (any, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var tasks []*task.Task
	for _, t := range i.db {
		tasks = append(tasks, t)
	}

	return tasks, nil
}

func (i *InMemoryTaskStore) Count() (int, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return len(i.db), nil
}

type InMemoryTaskEventStore struct {
	mu sync.RWMutex
	db map[string]*task.TaskEvent
}

func NewInMemoryTaskEventStore() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{
		db: make(map[string]*task.TaskEvent),
	}
}

func (i *InMemoryTaskEventStore) Put(key string, value any) error {
	e, ok := value.(*task.TaskEvent)
	if !ok || e == nil {
		return fmt.Errorf("value %v is not a task.TaskEvent type", value)
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if i.db == nil {
		i.db = make(map[string]*task.TaskEvent)
	}
	i.db[key] = e
	return nil
}

func (i *InMemoryTaskEventStore) Get(key string) (any, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	e, ok := i.db[key]
	if !ok {
		return nil, fmt.Errorf("task with key %s does not exist", key)
	}

	return e, nil
}

func (i *InMemoryTaskEventStore) List() (any, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var events []*task.TaskEvent
	for _, e := range i.db {
		events = append(events, e)
	}

	return events, nil
}

func (i *InMemoryTaskEventStore) Count() (int, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return len(i.db), nil
}
