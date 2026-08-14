package store

import "errors"

var ErrNotFound = errors.New("store: key not found")

const (
	MemoryType = "memory"
)

type Store[T any] interface {
	Put(key string, value *T) error
	Get(key string) (*T, error)
	List() ([]*T, error)
	Count() (int, error)
}
