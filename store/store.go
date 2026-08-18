package store

import (
	"errors"

	"github.com/google/uuid"
)

// Get never returns (nil, nil)
var ErrNotFound = errors.New("store: key not found")

const (
	MemoryType = "memory"
	BoltType   = "bolt"
)

type Store[T any] interface {
	Put(key uuid.UUID, value *T) error
	Get(key uuid.UUID) (*T, error)
	List() ([]*T, error)
	Count() (int, error)
	Delete(key uuid.UUID) error
	Close() error
}
