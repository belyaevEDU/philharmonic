package manager

import (
	"errors"
	"fmt"
	"log"

	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/google/uuid"
)

// task record lifecycle: acceptance into the pending queue, deletion, and reads

func (m *Manager) AddTask(te task.TaskEvent) error {
	// A task ID identifies one task lifecycle.
	// stops are allowed through even for legacy tasks whose mappings are no longer accepted,
	// so they can still clean up an existing container
	if te.State != task.Completed && te.Task.State != task.Completed {
		if err := task.ValidatePortMappings(te.Task.Ports); err != nil {
			return err
		}
		if err := task.ValidateRestartPolicy(te.Task.RestartPolicy); err != nil {
			return err
		}
		if te.Task.Timeout < 0 {
			return fmt.Errorf("task timeout must not be negative, got %d", te.Task.Timeout)
		}
		if te.Task.MaxRestarts < 0 {
			return fmt.Errorf("task max_restarts must not be negative, got %d", te.Task.MaxRestarts)
		}
		m.mu.Lock()
		if m.TaskDb == nil {
			m.mu.Unlock()
			return errors.New("task db is nil")
		}

		if _, err := m.TaskDb.Get(te.Task.ID); err == nil {
			m.mu.Unlock()
			return fmt.Errorf("task %s already exists", te.Task.ID)
		} else if !errors.Is(err, store.ErrNotFound) {
			m.mu.Unlock()
			return fmt.Errorf("checking task %s: %w", te.Task.ID, err)
		}

		// a task holds its Name for its whole lifecycle, including Completed.
		// Failed holds while it can still be restarted back to Scheduled and
		// reuse the name; once it reaches MaxRestarts, a stop can delete it.
		// Completed holds until the record is removed from the store:
		// a stop on an already-Completed task deletes it, freeing the name
		queued := te.Task
		if queued.Name != "" {
			// restricting the user from setting the task's name to be a UUID
			if isUUIDLike(queued.Name) {
				m.mu.Unlock()
				return fmt.Errorf("task name must not be a UUID: %q", queued.Name)
			}
			if clash := m.taskNameInUseLocked(queued.Name); clash {
				m.mu.Unlock()
				return fmt.Errorf("task name %q already in use", queued.Name)
			}
		}
		queued.State = task.Pending // updated, not yet sent to a worker
		if err := m.TaskDb.Put(queued.ID, &queued); err != nil {
			m.mu.Unlock()
			return fmt.Errorf("storing task %s: %w", queued.ID, err)
		}
		m.mu.Unlock()
	}

	m.enqueuePending(te)
	return nil
}

func isUUIDLike(name string) bool {
	_, err := uuid.Parse(name)
	return err == nil
}

// reports whether any task already holds the given Name.
// caller must hold m.mu
func (m *Manager) taskNameInUseLocked(name string) bool {
	persisted, err := m.TaskDb.List()
	if err != nil {
		log.Printf("Error listing tasks for name-uniqueness check: %v\n", err)
		return false // don't block the submit on a transient store error
	}
	for _, t := range persisted {
		if t == nil || t.Name != name {
			continue
		}
		return true // lol
	}
	return false
}

// resolves a ref (UUID or name) to a task via the shared task.ResolveRef
func (m *Manager) resolveTask(ref string) (task.Task, bool, bool) {
	return task.ResolveRef(m.getTasks(), ref)
}

// deleteTask removes a task record from the store and cleans up its pending,
// ownership, and port-reservation entries
func (m *Manager) deleteTask(t task.Task) error {
	m.mu.Lock()
	if m.TaskDb == nil {
		m.mu.Unlock()
		return errors.New("task db is nil")
	}
	if err := m.TaskDb.Delete(t.ID); err != nil {
		m.mu.Unlock()
		return err
	}

	var owner string
	if m.Assignments != nil {
		a, err := m.Assignments.Get(t.ID)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			log.Printf("Error reading assignment for task %s: %v\n", t.ID, err)
		}
		if a != nil {
			owner = a.Worker
		}
		if err := m.Assignments.Delete(t.ID); err != nil {
			log.Printf("Error deleting assignment for task %s: %v\n", t.ID, err)
		}
	}
	m.mu.Unlock()

	// durable deletion succeeded; clean up the rest
	m.removePendingTask(t.ID)
	if owner != "" {
		m.Reservations.Release(owner, &t)
	}
	if t.State == task.Failed && t.ContainerID != "" && owner != "" {
		m.bestEffortStopOldContainer(owner, t)
	}
	return nil
}

func (m *Manager) dequeuePending() (task.TaskEvent, bool) {
	return m.pending.Dequeue()
}

func (m *Manager) enqueuePending(te task.TaskEvent) {
	m.pending.Enqueue(te)
}

// drops every queued event for id and reports how many were dropped
func (m *Manager) removePendingTask(id uuid.UUID) int {
	return m.pending.RemoveAllFunc(func(te task.TaskEvent) bool {
		return te.Task.ID == id
	})
}

// all snapshots now to not worry about race conditions
func (m *Manager) getTasks() []task.Task {
	// pure reader: snapshot the pointer, never hold mu across store I/O
	m.mu.RLock()
	taskDb := m.TaskDb
	m.mu.RUnlock()

	if taskDb == nil {
		return []task.Task{}
	}

	persisted, err := taskDb.List()
	if err != nil {
		log.Printf("Error listing tasks: %v\n", err)
		return []task.Task{}
	}

	tasks := make([]task.Task, 0, len(persisted))
	for _, t := range persisted {
		if t != nil {
			tasks = append(tasks, *t)
		}
	}
	return tasks
}

func (m *Manager) getTaskViews() []TaskView {
	m.mu.RLock()
	taskDb, assignments := m.TaskDb, m.Assignments
	m.mu.RUnlock()

	if taskDb == nil {
		return []TaskView{}
	}

	persisted, err := taskDb.List()
	if err != nil {
		log.Printf("Error listing tasks: %v\n", err)
		return []TaskView{}
	}

	workerByTask := make(map[uuid.UUID]string)
	if assignments != nil {
		assignments, err := assignments.List()
		if err != nil {
			log.Printf("Error listing assignments: %v\n", err)
		}
		for _, a := range assignments {
			if a != nil {
				workerByTask[a.TaskID] = a.Worker
			}
		}
	}

	views := make([]TaskView, 0, len(persisted))
	for _, t := range persisted {
		if t == nil {
			continue
		}
		views = append(views, TaskView{
			Task:   *t,
			Worker: workerByTask[t.ID],
		})
	}
	return views
}

func (m *Manager) getNodeViews() []NodeView {
	m.mu.RLock()
	defer m.mu.RUnlock()

	views := make([]NodeView, 0, len(m.WorkerNodes))
	for _, n := range m.WorkerNodes {
		if n == nil {
			continue
		}
		views = append(views, NodeView{
			Snapshot: n.Snapshot(),
			Address:  n.Address,
			Role:     n.Role,
		})
	}
	return views
}

func (m *Manager) getTask(id uuid.UUID) (task.Task, bool) {
	m.mu.RLock()
	taskDb := m.TaskDb
	m.mu.RUnlock()

	if taskDb == nil {
		return task.Task{}, false
	}

	persisted, err := taskDb.Get(id)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("Error getting task %s: %v\n", id, err)
		}
		return task.Task{}, false
	}
	return *persisted, true
}
