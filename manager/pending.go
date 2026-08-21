package manager

import (
	"sync"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/google/uuid"
)

// replaces untyped golang-collections queue
type pendingQueue struct {
	mu     sync.Mutex
	events []task.TaskEvent
}

func (q *pendingQueue) enqueue(te task.TaskEvent) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.events = append(q.events, te)
}

// ok is false when the queue is empty
func (q *pendingQueue) dequeue() (te task.TaskEvent, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.events) == 0 {
		return task.TaskEvent{}, false
	}
	te = q.events[0]
	q.events[0] = task.TaskEvent{} // release the reference for gc
	q.events = q.events[1:]
	return te, true
}

func (q *pendingQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.events)
}

// drops every queued event belonging to task id
// and reports how many were dropped
func (q *pendingQueue) removeAll(id uuid.UUID) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	n := 0
	for _, e := range q.events {
		if e.Task.ID != id {
			q.events[n] = e
			n++
		}
	}
	dropped := len(q.events) - n
	for i := n; i < len(q.events); i++ {
		q.events[i] = task.TaskEvent{} // release dropped references for gc
	}
	q.events = q.events[:n]
	return dropped
}
