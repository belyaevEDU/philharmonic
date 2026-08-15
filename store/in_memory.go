package store

import (
	"bytes"
	"errors"
	"slices"
	"sync"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/google/uuid"
)

type InMemoryTaskStore struct {
	mu sync.RWMutex
	db map[uuid.UUID]*task.Task
}

// compile-time interface satisfaction check
var _ Store[task.Task] = (*InMemoryTaskStore)(nil)

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		db: make(map[uuid.UUID]*task.Task),
	}
}

func (i *InMemoryTaskStore) Put(key uuid.UUID, value *task.Task) error {
	if value == nil {
		return errors.New("store: cannot put nil task")
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if i.db == nil {
		i.db = make(map[uuid.UUID]*task.Task)
	}
	i.db[key] = value
	return nil
}

func (i *InMemoryTaskStore) Get(key uuid.UUID) (*task.Task, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	t, ok := i.db[key]
	if !ok {
		return nil, ErrNotFound
	}

	return t, nil
}

func (i *InMemoryTaskStore) List() ([]*task.Task, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	tasks := make([]*task.Task, 0, len(i.db))
	for _, t := range i.db {
		tasks = append(tasks, t)
	}

	// map iteration order is randomized. sort by ID so API output & the
	// manager's reconcile/restart passes are deterministic
	slices.SortFunc(tasks, func(a, b *task.Task) int {
		return bytes.Compare(a.ID[:], b.ID[:])
	})

	return tasks, nil
}

func (i *InMemoryTaskStore) Count() (int, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return len(i.db), nil
}

func (i *InMemoryTaskStore) Close() error { return nil }

type InMemoryTaskEventStore struct {
	mu sync.RWMutex
	db map[uuid.UUID]*task.TaskEvent
}

var _ Store[task.TaskEvent] = (*InMemoryTaskEventStore)(nil)

func NewInMemoryTaskEventStore() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{
		db: make(map[uuid.UUID]*task.TaskEvent),
	}
}

func (i *InMemoryTaskEventStore) Put(key uuid.UUID, value *task.TaskEvent) error {
	if value == nil {
		return errors.New("store: cannot put nil task event")
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if i.db == nil {
		i.db = make(map[uuid.UUID]*task.TaskEvent)
	}
	i.db[key] = value
	return nil
}

func (i *InMemoryTaskEventStore) Get(key uuid.UUID) (*task.TaskEvent, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	e, ok := i.db[key]
	if !ok {
		return nil, ErrNotFound
	}

	return e, nil
}

func (i *InMemoryTaskEventStore) List() ([]*task.TaskEvent, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	events := make([]*task.TaskEvent, 0, len(i.db))
	for _, e := range i.db {
		events = append(events, e)
	}

	slices.SortFunc(events, func(a, b *task.TaskEvent) int {
		return bytes.Compare(a.ID[:], b.ID[:])
	})

	return events, nil
}

func (i *InMemoryTaskEventStore) Count() (int, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return len(i.db), nil
}

func (i *InMemoryTaskEventStore) Close() error { return nil }
