package store

const (
	MemoryType = "memory"
)

type Store interface {
	Put(key string, value any) error
	Get(key string) (any, error)
	List() (any, error)
	Count() (int, error)
}
