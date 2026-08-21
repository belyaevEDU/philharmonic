package manager

import (
	"github.com/belyaevedu/philharmonic/store"
	"github.com/google/uuid"
)

type Assignment struct {
	TaskID uuid.UUID
	Worker string
}

func (a Assignment) Key() uuid.UUID { return a.TaskID }

var (
	_ store.Keyable           = Assignment{}
	_ store.Store[Assignment] = (*store.InMemoryStore[Assignment])(nil)
	_ store.Store[Assignment] = (*store.BoltStore[Assignment])(nil)
)
