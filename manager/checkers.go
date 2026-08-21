package manager

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// identifies one checker instance.
// pointer identity is the identity of the checker,
// so a stale handle must never tear down a newer checker's registration
type checkerHandle struct {
	cancel context.CancelFunc
}

type checkerSet struct {
	mu     sync.Mutex
	byTask map[uuid.UUID]*checkerHandle
}

func newCheckerSet() *checkerSet {
	return &checkerSet{byTask: make(map[uuid.UUID]*checkerHandle)}
}

// registers a fresh checker for the task
// reports false if one is already running
func (cs *checkerSet) start(id uuid.UUID, h *checkerHandle) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if _, running := cs.byTask[id]; running {
		return false
	}
	cs.byTask[id] = h
	return true
}

// cancels and forgets the checker for the task, if there is any
func (cs *checkerSet) stop(id uuid.UUID) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if h, ok := cs.byTask[id]; ok {
		h.cancel()
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
	for id, h := range cs.byTask {
		if _, ok := keep[id]; ok {
			continue
		}
		h.cancel()
		delete(cs.byTask, id)
		stopped = append(stopped, id)
	}
	return stopped
}

// tears down a checker whose goroutine has exited:
// releases its ctx and drops its registration so the next
// reconcileCheckers pass can start a fresh one.
// without this, a self-exited checker leaves a stale entry behind
// and a still-Running task goes unmonitored forever
func (cs *checkerSet) finished(id uuid.UUID, h *checkerHandle) {
	h.cancel()

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cur, ok := cs.byTask[id]; ok && cur == h {
		delete(cs.byTask, id)
	}
}
