package manager

import (
	"errors"
	"log"

	"github.com/belyaevedu/philharmonic/store"
	"github.com/google/uuid"
)

// task ownership: which worker currently runs a task

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

// sweeps assignments whose task record is gone
// a crash between the task delete and the assignment delete in deleteTask leaves the assignment behind
func sweepOrphanedAssignments(assignmentsStore store.Store[Assignment], knownTasks map[uuid.UUID]struct{}) {
	if assignmentsStore == nil {
		return
	}
	assignments, err := assignmentsStore.List()
	if err != nil {
		log.Printf("Could not list assignments for the startup orphan sweep; orphans (if any) remain: %v\n", err)
		return
	}
	for _, a := range assignments {
		if a == nil {
			continue
		}
		if _, ok := knownTasks[a.TaskID]; ok {
			continue
		}
		if err := assignmentsStore.Delete(a.TaskID); err != nil {
			log.Printf("Could not delete orphaned assignment for missing task %s (worker %q); it remains and will be re-swept on the next restart: %v\n", a.TaskID, a.Worker, err)
			continue
		}
		log.Printf("Swept orphaned assignment for missing task %s (worker %q)\n", a.TaskID, a.Worker)
	}
}

func (m *Manager) taskWorker(id uuid.UUID) string {
	// pure reader: snapshot the pointer, never hold mu across store I/O
	m.mu.RLock()
	assignments := m.Assignments
	m.mu.RUnlock()
	return readAssignment(assignments, id)
}

// reads the persisted owner of a task from a snapshot store pointer.
// callers holding mu may use it directly; pure readers must pass a snapshot
func readAssignment(assignments store.Store[Assignment], id uuid.UUID) string {
	if assignments == nil {
		return ""
	}
	a, err := assignments.Get(id)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("Error reading assignment for task %s: %v\n", id, err)
		}
		return ""
	}
	return a.Worker
}

// persists the owner of a task
// store.Store.Put is idempotent, so recommitting an unchanged owner is a no-op by design
func (m *Manager) setAssignment(id uuid.UUID, worker string) error {
	if m.Assignments == nil {
		return errors.New("assignments db is nil")
	}
	return m.Assignments.Put(id, &Assignment{TaskID: id, Worker: worker})
}

// reads an assignment while the caller holds m.mu,
// preserving whether the record existed so a failed commit can restore the prior state
func readAssignmentState(assignments store.Store[Assignment], id uuid.UUID) (Assignment, bool, error) {
	if assignments == nil {
		return Assignment{}, false, errors.New("assignments db is nil")
	}
	a, err := assignments.Get(id)
	if errors.Is(err, store.ErrNotFound) {
		return Assignment{}, false, nil
	}
	if err != nil {
		return Assignment{}, false, err
	}
	if a == nil {
		return Assignment{}, false, errors.New("assignments store returned a nil record")
	}
	return *a, true, nil
}

// restores an assignment while the caller holds m.mu
func restoreAssignment(assignments store.Store[Assignment], id uuid.UUID, old Assignment, existed bool) error {
	if assignments == nil {
		return errors.New("assignments db is nil")
	}
	if !existed {
		return assignments.Delete(id)
	}
	return assignments.Put(id, &old)
}
