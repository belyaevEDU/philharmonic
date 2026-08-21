package queue

import "sync"

// mutex-guarded FIFO queue of T
type Queue[T any] struct {
	mu    sync.Mutex
	items []T
}

func New[T any]() *Queue[T] {
	return &Queue[T]{}
}

func (q *Queue[T]) Enqueue(item T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, item)
}

// ok is false when the queue is empty
func (q *Queue[T]) Dequeue() (item T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return item, false
	}
	item = q.items[0]
	var zero T
	q.items[0] = zero // release the reference for gc
	q.items = q.items[1:]
	return item, true
}

func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// drops every item for which match reports true and returns how many were dropped
func (q *Queue[T]) RemoveAllFunc(match func(T) bool) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	n := 0
	for _, item := range q.items {
		if !match(item) {
			q.items[n] = item
			n++
		}
	}
	removed := len(q.items) - n
	var zero T
	for i := n; i < len(q.items); i++ {
		q.items[i] = zero // release dropped references for gc
	}
	q.items = q.items[:n]
	return removed
}
