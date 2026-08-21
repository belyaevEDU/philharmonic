package manager

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type checkerSet struct {
	mu     sync.Mutex
	byTask map[uuid.UUID]context.CancelFunc
}

func newCheckerSet() *checkerSet {
	return &checkerSet{byTask: make(map[uuid.UUID]context.CancelFunc)}
}

// registers a fresh checker for the task
// reports false if one is already running,
// in which case cancel is NOT invoked and the existing checker keeps running
func (cs *checkerSet) start(id uuid.UUID, cancel context.CancelFunc) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if _, running := cs.byTask[id]; running {
		return false
	}
	cs.byTask[id] = cancel
	return true
}

// cancels and forgets the checker for the task, if there is any
func (cs *checkerSet) stop(id uuid.UUID) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cancel, ok := cs.byTask[id]; ok {
		cancel()
		delete(cs.byTask, id)
	}
}

func (cs *checkerSet) has(id uuid.UUID) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	_, ok := cs.byTask[id]
	return ok
}

// cancels and forgets every checker whose task ID is not in keep,
// returning the stopped IDs
//
// used to tear down checkers
// whose tasks are no longer running or no longer manager-checked
func (cs *checkerSet) stopAllExcept(keep map[uuid.UUID]struct{}) []uuid.UUID {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	var stopped []uuid.UUID
	for id, cancel := range cs.byTask {
		if _, ok := keep[id]; ok {
			continue
		}
		cancel()
		delete(cs.byTask, id)
		stopped = append(stopped, id)
	}
	return stopped
}
