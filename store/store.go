package store

import (
	"fmt"

	"github.com/belyaevedu/philharmonic/task"
)

type Store interface {
	Put(key string, value any) error
	Get(key string) (any, error)
	List() (any, error)
	Count() (int, error)
}

type InMemoryTaskStore struct {
	db map[string]*task.Task
}

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		db: make(map[string]*task.Task),
	}
}

func (i *InMemoryTaskStore) Put(key string, value any) error {
	t, ok := value.(*task.Task)
	if ok {
		return fmt.Errorf("value %v is not a task.Task type", value)
	}

	i.db[key] = t
	return nil
}

func (i *InMemoryTaskStore) Get(key string) (any, error) {
	t, ok := i.db[key]
	if !ok {
		return nil, fmt.Errorf("task with key %s does not exist", key)
	}

	return t, nil
}

func (i *InMemoryTaskStore) List() (any, error) {
	var tasks []*task.Task
	for _, t := range i.db {
		tasks = append(tasks, t)
	}

	return tasks, nil
}

func (i *InMemoryTaskStore) Count() (int, error) {
	return len(i.db), nil
}
