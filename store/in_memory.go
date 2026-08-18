package store

import (
	"bytes"
	"fmt"
	"slices"
	"sync"

	"github.com/google/uuid"
)

// need this for List to be deterministic
// since i sort the slice by the key
type Keyable interface {
	Key() uuid.UUID
}

type InMemoryStore[T Keyable] struct {
	mu sync.RWMutex
	db map[uuid.UUID]*T
}

func NewInMemoryStore[T Keyable]() *InMemoryStore[T] {
	return &InMemoryStore[T]{db: make(map[uuid.UUID]*T)}
}

func (i *InMemoryStore[T]) Put(key uuid.UUID, value *T) error {
	if value == nil {
		return fmt.Errorf("store: cannot put nil %T", value)
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if i.db == nil {
		i.db = make(map[uuid.UUID]*T)
	}
	i.db[key] = value
	return nil
}

func (i *InMemoryStore[T]) Get(key uuid.UUID) (*T, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	v, ok := i.db[key]
	if !ok {
		return nil, ErrNotFound
	}

	return v, nil
}

func (i *InMemoryStore[T]) List() ([]*T, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	out := make([]*T, 0, len(i.db))
	for _, v := range i.db {
		out = append(out, v)
	}

	// map iteration order is randomized. sort by key so API output and the
	// manager's reconcile/restart passes are deterministic, matching bbolt's
	// natural key-bytes ordering.
	slices.SortFunc(out, func(a, b *T) int {
		ka, kb := (*a).Key(), (*b).Key()
		return bytes.Compare(ka[:], kb[:])
	})

	return out, nil
}

func (i *InMemoryStore[T]) Count() (int, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return len(i.db), nil
}

func (i *InMemoryStore[T]) Delete(key uuid.UUID) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.db, key)
	return nil
}

func (i *InMemoryStore[T]) Close() error { return nil }
