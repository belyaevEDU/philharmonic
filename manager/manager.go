package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/belyaevedu/philharmonic/handlers"
	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/queue"
	"github.com/belyaevedu/philharmonic/scheduler"
	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/google/uuid"
)

const (
	WorkerRole = "worker"

	DbFilemode = os.FileMode(0600)

	maxProxyLogSize = 5 << 20 // 5 MiB
)

var (
	MaxRestarts = 3

	LoopInterval = 10 * time.Second

	DbFile = "philharmonic.db"

	DbTaskBucket       = "tasks"
	DbEventBucket      = "events"
	DbAssignmentBucket = "assignments"
)

type Manager struct {
	pending      *queue.Queue[task.TaskEvent]
	TaskDb       store.Store[task.Task]
	EventDb      store.Store[task.TaskEvent]
	Assignments  store.Store[Assignment]
	Reservations ReservationStore
	checkers     *checkerSet

	// owns the single bbolt file backing all three stores in bolt mode.
	// is nil for memory mode
	// the stores themselves are bucket views on it
	boltDb *store.SharedBolt

	// guards the store pointers above (against Close nil-ing them)
	// and serializes read-modify-write sequences across stores
	//
	// locking rules:
	// - read-modify-write sequences (accept, update, delete, commit)
	//   hold mu across their store I/O
	// - pure readers snapshot the store pointer under mu, release it, and
	//   never hold mu during store I/O to not stall other processes
	mu sync.RWMutex

	WorkerNodes []*node.Node
	Scheduler   scheduler.Scheduler
}

func New(workers []string, schedulerType, dbType string) (*Manager, error) {
	var nodes []*node.Node
	for _, worker := range workers {
		n, err := node.New(worker, WorkerRole)
		if err != nil {
			return nil, fmt.Errorf("invalid worker %q: %w", worker, err)
		}
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case "", scheduler.EpvmDefaultName:
		s = scheduler.NewEpvm()
	case scheduler.RoundRobinDefaultName:
		s = scheduler.NewRoundRobin()
	default:
		return nil, fmt.Errorf("unknown scheduler type %q: want %q or %q",
			schedulerType, scheduler.EpvmDefaultName, scheduler.RoundRobinDefaultName)
	}

	m := Manager{
		pending:      queue.New[task.TaskEvent](),
		Reservations: NewReservationTable(),
		checkers:     newCheckerSet(),
		WorkerNodes:  nodes,
		Scheduler:    s,
	}

	var ts store.Store[task.Task]
	var es store.Store[task.TaskEvent]
	var as store.Store[Assignment]
	switch dbType {
	case store.MemoryType:
		ts = store.NewInMemoryStore[task.Task]()
		es = store.NewInMemoryStore[task.TaskEvent]()
		as = store.NewInMemoryStore[Assignment]()
	case store.BoltType:
		sdb, dbErr := store.OpenSharedBolt(DbFile, DbFilemode)
		if dbErr != nil {
			return nil, fmt.Errorf("opening %s: %w", DbFile, dbErr)
		}
		ts, dbErr = store.Bucket[task.Task](sdb, DbTaskBucket)
		if dbErr != nil {
			return nil, errors.Join(fmt.Errorf("opening tasks bucket: %w", dbErr), sdb.Close())
		}
		es, dbErr = store.Bucket[task.TaskEvent](sdb, DbEventBucket)
		if dbErr != nil {
			return nil, errors.Join(fmt.Errorf("opening events bucket: %w", dbErr), sdb.Close())
		}
		as, dbErr = store.Bucket[Assignment](sdb, DbAssignmentBucket)
		if dbErr != nil {
			return nil, errors.Join(fmt.Errorf("opening assignments bucket: %w", dbErr), sdb.Close())
		}
		m.boltDb = sdb
	default:
		return nil, errors.New("unknown db type given")
	}

	m.TaskDb = ts
	m.EventDb = es
	m.Assignments = as
	if err := m.reconcileOnStartup(); err != nil {
		// the manager must not run with unreconciled state
		// it would silently double-book host ports or leave accepted tasks to be
		return nil, errors.Join(fmt.Errorf("startup reconciliation: %w", err), m.Close())
	}
	return &m, nil
}

func (m *Manager) reconcileOnStartup() error {
	persisted, err := m.TaskDb.List()
	if err != nil {
		return fmt.Errorf("listing tasks: %w", err)
	}

	knownTasks := make(map[uuid.UUID]struct{}, len(persisted))
	var (
		recoveredRunning   int
		recoveredScheduled int
		requeuedPending    []uuid.UUID
	)
	for _, t := range persisted {
		if t == nil {
			continue
		}
		knownTasks[t.ID] = struct{}{}

		switch t.State {
		case task.Pending:
			m.enqueuePending(task.TaskEvent{
				ID:        uuid.New(),
				State:     task.Scheduled,
				Timestamp: time.Now().UTC(),
				Task:      *t,
			})
			requeuedPending = append(requeuedPending, t.ID)

		case task.Running, task.Scheduled:
			// single-threaded here as New is intended to be the only caller, so a direct read is safe
			owner := readAssignment(m.Assignments, t.ID)
			if owner == "" {
				log.Printf("Recovered %s task %s has no recorded worker; it will be placed by the normal scheduling flows\n", t.State, t.ID)
				continue
			}
			if t.State == task.Running {
				recoveredRunning++
			} else {
				recoveredScheduled++
			}
			if m.workerByAddress(owner) == nil {
				log.Printf("Recovered %s task %s points at unconfigured worker %q; dispatch will retry until the worker joins or the task is stopped\n", t.State, t.ID, owner)
			}
			if hasPinnedHostPorts(t) && !m.Reservations.TryReserve(owner, t) {
				log.Printf("Conflicting restored port reservations for recovered task %s on %s; deferring to the workers' live port inventories\n", t.ID, owner)
			}
		}
	}

	if n := len(requeuedPending); n > 0 {
		log.Printf("Requeued %d task(s) that were accepted but not yet dispatched before the restart\n", n)
	}
	if recoveredRunning+recoveredScheduled > 0 {
		log.Printf("Recovered %d running and %d scheduled task(s) from the previous run\n", recoveredRunning, recoveredScheduled)
	}

	sweepOrphanedAssignments(m.Assignments, knownTasks)
	return nil
}

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

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TaskDb = nil
	m.EventDb = nil
	m.Assignments = nil

	var err error
	if m.boltDb != nil {
		err = m.boltDb.Close()
		m.boltDb = nil
	}
	return err
}

func (m *Manager) SelectWorker(t *task.Task) (*node.Node, error) {
	m.Reservations.Lock()
	defer m.Reservations.Unlock()
	return m.selectWorkerLocked(t, "")
}

// filters candidates using a complete port inventory and the manager's reservations
// the caller must hold m.Reservations
func (m *Manager) selectWorkerLocked(t *task.Task, selfOwner string) (*node.Node, error) {
	if t == nil {
		return nil, errors.New("cannot select a worker for a nil task")
	}
	if err := task.ValidatePortMappings(t.Ports); err != nil {
		return nil, err
	}

	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available candidates match resource requests for task %s", t.ID)
	}

	able := make([]*node.Node, 0, len(candidates))
	for _, c := range candidates {
		excludeOwnPorts := selfOwner != "" && c.Address == selfOwner
		if hasPinnedHostPorts(t) {
			occ, err := m.fetchWorkerPorts(c)
			if err != nil {
				log.Printf("Skipping worker %s during port admission: %v\n", c.Address, err)
				continue
			}
			if !m.canHost(t, occ, excludeOwnPorts) {
				continue
			}
		}
		if m.Reservations.ConflictsLocked(c.Address, t, false) {
			continue
		}
		able = append(able, c)
	}
	if len(able) == 0 {
		return nil, fmt.Errorf(
			"no worker can host task %s: every candidate has a conflicting or unavailable host port",
			t.ID,
		)
	}

	scores := m.Scheduler.Score(t, able)
	selectedNode := m.Scheduler.Pick(scores, able)
	if selectedNode == nil {
		return nil, fmt.Errorf("no candidate able to host task %s (all unscoreable)", t.ID)
	}
	return selectedNode, nil
}

func (m *Manager) selectAndReserveWorker(t *task.Task, owner string) (*node.Node, error) {
	m.Reservations.Lock()
	defer m.Reservations.Unlock()

	if err := task.ValidatePortMappings(t.Ports); err != nil {
		return nil, err
	}
	if owner != "" { // restart branch
		ownerNode := m.workerByAddress(owner)
		if ownerNode != nil && !m.Reservations.ConflictsLocked(owner, t, true) {
			if !hasPinnedHostPorts(t) {
				return ownerNode, nil
			}
			occ, err := m.fetchWorkerPorts(ownerNode)
			if err == nil && m.canHost(t, occ, true) && m.Reservations.TryReserveLocked(owner, t) {
				return ownerNode, nil
			}
			if err != nil {
				log.Printf("Cannot use owning worker %s during port admission: %v\n", owner, err)
			}
		}
	}

	selected, err := m.selectWorkerLocked(t, "")
	if err != nil {
		return nil, err
	}
	// impossible while the table's lock is held and selectWorkerLocked checked
	// conflicts for every candidate; handled defensively anyway
	if !m.Reservations.TryReserveLocked(selected.Address, t) {
		return nil, fmt.Errorf("lost race reserving host ports for task %s on %s", t.ID, selected.Address)
	}
	return selected, nil
}

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

func (m *Manager) pendingLen() int {
	return m.pending.Len()
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

func (m *Manager) workerByAddress(address string) *node.Node {
	for _, worker := range m.WorkerNodes {
		if worker.Address == address {
			return worker
		}
	}
	return nil
}

func (m *Manager) SendWork() {
	te, ok := m.dequeuePending()
	if !ok {
		log.Println("No work in the queue")
		return
	}

	t := te.Task
	isStop := te.State == task.Completed || t.State == task.Completed

	var w *node.Node
	reserved := false
	owner := m.taskWorker(t.ID)
	if owner != "" {
		w = m.workerByAddress(owner)
		if w == nil {
			log.Printf("Cannot send task %s: assigned worker %q is unavailable\n", t.ID, owner)
			m.enqueuePending(te)
			return
		}
		if !isStop {
			reserved = m.Reservations.TryReserve(w.Address, &t)
			if !reserved {
				log.Printf("Cannot reserve host ports for task %s on worker %s\n", t.ID, w.Address)
				m.markFailed(t.ID, t.RestartCount+1, "host port is reserved by another task")
				return
			}
		}
	} else if isStop {
		// a stop can overtake a failed/requeued start while the task is still Pending.
		// there is no worker to contact in that case; cancel the queued start
		// and remove the task instead of dropping the stop
		if pending, exists := m.getTask(t.ID); exists && pending.State == task.Pending {
			if err := m.deleteTask(pending); err != nil {
				log.Printf("Error deleting pending task %s: %v\n", t.ID, err)
				m.enqueuePending(te)
			} else {
				log.Printf("Cancelled pending task %s\n", t.ID)
			}
			return
		}
		log.Printf("Cannot stop task %s: no worker owns it\n", t.ID)
		return
	} else {
		// a cancellation may have removed the task while it was sitting in the
		// queue; skip dispatch rather than creating a container we'd have to
		// stop immediately. (the post-201 check below is the correctness backstop
		// if the deletion races with this check). nightmare
		if _, exists := m.getTask(t.ID); !exists {
			log.Printf("Task %s was cancelled while queued, skipping dispatch\n", t.ID)
			return
		}
		var err error
		w, err = m.selectAndReserveWorker(&t, "")
		if err != nil {
			log.Printf("Error selecting worker for task %s: %v\n", t.ID, err)
			m.enqueuePending(te)
			return
		}
		reserved = true
	}

	if !isStop {
		t.State = task.Scheduled
		te.Task = t
		te.State = task.Scheduled
		if te.Timestamp.IsZero() {
			te.Timestamp = time.Now().UTC()
		}
	}
	if isStop {
		log.Printf("Stopping task %s on worker %s\n", t.ID, w.Address)
	} else if t.RestartCount > 0 {
		log.Printf("Restarting task %s on worker %s\n", t.ID, w.Address)
	} else {
		log.Printf("Starting task %s on worker %s\n", t.ID, w.Address)
	}

	data, err := json.Marshal(te)
	if err != nil {
		if reserved {
			m.Reservations.Release(w.Address, &t)
		}
		log.Printf("Error raised when marshalling task object %v: %v\n", t, err)
		return
	}

	url := httpclient.WorkerURL(w.Address, "/tasks")
	// ignoring gosec's G107 since the url is not from external input, but from an internal config
	resp, err := httpclient.Worker().Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		if reserved {
			m.Reservations.Release(w.Address, &t)
		}
		fmt.Printf("Error connecting to %v: %v\n", w, err)
		m.enqueuePending(te)
		return
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		if reserved {
			m.Reservations.Release(w.Address, &t)
		}
		hr := handlers.HTTPResponse{}
		if err := d.Decode(&hr); err != nil {
			fmt.Printf("Error decoding response: %v\n", err)
		} else {
			log.Printf("Response error (%d): %s", hr.HTTPStatusCode, hr.Message)
		}
		if isStop {
			// a rejected stop leaves the task running.
			// we neither requeue (would loop forever against the fixed owning worker)
			// nor mark Failed (would make restartFailedTasks try to *restart* a task
			// the user asked to stop). just log it and hope for the best
			return
		}
		// rejection of a start/restart: marking it failed w/o term to try & restart again
		reason := fmt.Sprintf("worker %s rejected task (%d)", w.Address, resp.StatusCode)
		if hr.Message != "" {
			reason = fmt.Sprintf("%s: %s", reason, hr.Message)
		}
		m.markFailed(t.ID, t.RestartCount+1, reason)
		return
	}

	respTask := task.Task{}
	decodeErr := d.Decode(&respTask)
	if decodeErr != nil {
		fmt.Printf("Error decoding response: %v\n", decodeErr)
	}

	m.mu.Lock()
	if m.EventDb == nil {
		log.Printf("Cannot store event %s: event db is nil\n", te.ID)
	} else if err := m.EventDb.Put(te.ID, &te); err != nil {
		log.Printf("Error storing event %s: %v\n", te.ID, err)
	}
	if !isStop {
		// a cancellation may have deleted the task while the POST was in flight.
		// if so, don't resurrect it. release the ports we reserved and issue a
		// compensating stop for the container the worker just created
		if _, getErr := m.TaskDb.Get(t.ID); getErr != nil {
			m.mu.Unlock()
			if reserved {
				m.Reservations.Release(w.Address, &t)
			}
			log.Printf("Task %s was cancelled while starting on worker %s; stopping orphaned container\n", t.ID, w.Address)
			m.bestEffortStopOldContainer(w.Address, respTask)
			return
		}
		if err := m.setAssignment(t.ID, w.Address); err != nil {
			log.Printf("Error storing assignment for task %s: %v\n", t.ID, err)
		}
		if m.TaskDb == nil {
			log.Printf("Cannot store task %s: task db is nil\n", t.ID)
		} else if err := m.TaskDb.Put(t.ID, &t); err != nil {
			log.Printf("Error storing task %s: %v\n", t.ID, err)
		}
	}
	m.mu.Unlock()

	if isStop {
		m.Reservations.Release(w.Address, &t)
	}

	if decodeErr == nil {
		log.Printf("%#v\n", respTask) // # adds field names
	}
}

func (m *Manager) fetchTasksFromWorker(worker string) ([]*task.Task, error) {
	url := httpclient.WorkerURL(worker, "/tasks")
	// ignoring gosec's G107 since the url is not from external input, but from an internal config
	resp, err := httpclient.Worker().Get(url) // #nosec G107
	if err != nil {
		return nil, fmt.Errorf("error connecting to worker: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("worker responded with status %d", resp.StatusCode)
	}

	var tasks []*task.Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, fmt.Errorf("error unmarshalling tasks: %w", err)
	}
	return tasks, nil
}

type WorkerLogsResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// proxies a logs request to the owning worker
func (m *Manager) fetchTaskLogsFromWorker(worker string, taskID uuid.UUID, tail int) (*WorkerLogsResponse, error) {
	path := "/tasks/logs/" + taskID.String()
	if tail > 0 {
		path += "?tail=" + strconv.Itoa(tail)
	}
	url := httpclient.WorkerURL(worker, path)
	// ignoring gosec's G107/G704: no request-controlled input reaches this URL
	resp, err := httpclient.Worker().Get(url) // #nosec G107 G704
	if err != nil {
		return nil, fmt.Errorf("error connecting to worker: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing logs response body from worker %s: %v\n", worker, err)
		}
	}()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProxyLogSize))
	if err != nil {
		return nil, fmt.Errorf("error reading logs response body from worker %s: %w", worker, err)
	}
	return &WorkerLogsResponse{StatusCode: resp.StatusCode, Header: resp.Header, Body: body}, nil
}

func (m *Manager) updateTasks() {
	for _, n := range m.WorkerNodes {
		tasks, err := m.fetchTasksFromWorker(n.Address)
		if err != nil {
			log.Printf("Error fetching tasks from %s: %v\n", n.Address, err)
			continue
		}

		for _, t := range tasks {
			m.mu.Lock()
			if m.TaskDb == nil {
				m.mu.Unlock()
				log.Printf("Task db is nil; cannot update task %s\n", t.ID.String())
				continue
			}

			persisted, err := m.TaskDb.Get(t.ID)
			if err != nil {
				m.mu.Unlock()
				if !errors.Is(err, store.ErrNotFound) {
					log.Printf("Error getting task %s: %v\n", t.ID.String(), err)
				} else {
					log.Printf("Task with ID %s not found\n", t.ID.String())
				}
				continue
			}

			// a worker can still be returning a snapshot from the previous restart attempt
			// while the manager has already queued the next one
			if t.RestartCount != persisted.RestartCount {
				m.mu.Unlock()
				continue
			}

			updated := *persisted
			updated.ContainerID = t.ContainerID
			updated.HostPorts = t.HostPorts
			if !t.StartTime.IsZero() {
				updated.StartTime = t.StartTime
			}
			if !t.FinishTime.IsZero() {
				updated.FinishTime = t.FinishTime
			}

			if t.State == task.Failed {
				updated.State = task.Failed
				if t.FailureReason != "" {
					updated.FailureReason = t.FailureReason
				}
			} else if updated.State != task.Failed || updated.FinishTime.IsZero() {
				// not terminal-failed -> trust the worker's state
				updated.State = t.State
			}
			// else: terminal-failed here

			changed := !reflect.DeepEqual(*persisted, updated)
			if err := m.TaskDb.Put(t.ID, &updated); err != nil {
				log.Printf("Error updating task %s: %v\n", t.ID, err)
			} else if changed && persisted.State != updated.State {
				log.Printf("Task %s changed state from %s to %s\n", t.ID, persisted.State, updated.State)
			} else if changed {
				log.Printf("Task %s changed\n", t.ID)
			}
			m.mu.Unlock()
		}
	}
}

func (m *Manager) UpdateTasks(ctx context.Context) {
	ticker := time.NewTicker(LoopInterval)
	defer ticker.Stop()
	for {
		m.updateTasks()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) DoHealthChecks(ctx context.Context) {
	ticker := time.NewTicker(LoopInterval)
	defer ticker.Stop()
	for {
		m.reconcileCheckers(ctx)
		m.restartFailedTasks()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) RefreshNodeStats(ctx context.Context) {
	ticker := time.NewTicker(LoopInterval)
	defer ticker.Stop()
	for {
		for _, n := range m.WorkerNodes {
			if _, err := n.GetStats(); err != nil {
				log.Printf("Error refreshing stats for node %s: %v", n.Address, err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) restartTask(t task.Task) error {
	// restartTask schedules a restart of t on its owning worker
	// for a task with no owner re-schedules it via the scheduler
	owner := m.taskWorker(t.ID)

	next := t
	next.State = task.Scheduled
	next.RestartCount = t.RestartCount + 1
	next.FailureReason = ""
	next.StartTime = time.Time{}
	next.FinishTime = time.Time{}

	w, err := m.selectAndReserveWorker(&next, owner)
	if err != nil {
		reason := fmt.Sprintf("no available worker to restart: %v", err)
		m.stopTaskTerminal(t, reason)
		return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
	}

	if owner == "" || w.Address != owner {
		next.ContainerID = ""
		next.HostPorts = nil
	}

	log.Printf("Restarting task %s on worker %s\n", t.ID, w.Address)

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Scheduled,
		Timestamp: time.Now().UTC(),
		Task:      next,
	}

	data, err := json.Marshal(te)
	if err != nil {
		m.Reservations.Release(w.Address, &next)
		reason := fmt.Sprintf("unable to marshal restart event: %v", err)
		m.markFailed(t.ID, t.RestartCount+1, reason)
		return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
	}

	url := httpclient.WorkerURL(w.Address, "/tasks")
	// ignoring gosec's G107 since the url is not from external input, but from an internal config
	resp, err := httpclient.Worker().Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		m.Reservations.Release(w.Address, &next)
		// the worker never saw the restart, so don't burn a restart slot
		reason := fmt.Sprintf("could not reach worker %s to restart: %v", w.Address, err)
		m.markFailed(t.ID, t.RestartCount, reason)
		return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		m.Reservations.Release(w.Address, &next)
		hr := handlers.HTTPResponse{}
		if err := d.Decode(&hr); err != nil {
			log.Printf("Error decoding rejection response: %v\n", err)
		}
		// the worker refused the restart, so burn a slot and
		// let restartFailedTasks bound the retries via MaxRestarts
		reason := fmt.Sprintf("worker %s rejected restart (%d)", w.Address, resp.StatusCode)
		if hr.Message != "" {
			reason = fmt.Sprintf("%s: %s", reason, hr.Message)
		}
		m.markFailed(t.ID, t.RestartCount+1, reason)
		return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
	}

	m.mu.Lock()
	oldOwner := readAssignment(m.Assignments, t.ID)
	if err := m.setAssignment(t.ID, w.Address); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("cannot store assignment for restarted task %s: %w", t.ID, err)
	}
	if m.TaskDb == nil {
		m.mu.Unlock()
		return fmt.Errorf("cannot store restarted task %s: task db is nil", t.ID)
	}
	if err := m.TaskDb.Put(next.ID, &next); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("cannot store restarted task %s: %w", t.ID, err)
	}
	m.mu.Unlock()

	if oldOwner != "" && oldOwner != w.Address {
		m.Reservations.Release(oldOwner, &t)
		m.bestEffortStopOldContainer(oldOwner, t)
	}

	newTask := task.Task{}
	if err := d.Decode(&newTask); err != nil {
		log.Printf("Error decoding restart response: %v\n", err)
		// ownership already committed via 201, so we dont really care about a json decoding error
		return nil
	}

	log.Printf("Restarted task %s on worker %s\n", t.ID, w.Address)
	return nil
}

// markFailed records a task as Failed with the given restart count and reason
// clearing any prior terminal stamp (FinishTime = 0)
// so restartFailedTasks can drive the task again
func (m *Manager) markFailed(id uuid.UUID, restartCount int, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.TaskDb == nil {
		log.Printf("Cannot mark task %s as failed: task db is nil\n", id)
		return
	}

	persisted, err := m.TaskDb.Get(id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			persisted = &task.Task{ID: id}
		} else {
			log.Printf("Cannot mark task %s as failed: %v\n", id, err)
			return
		}
	}

	updated := *persisted
	updated.State = task.Failed
	updated.RestartCount = restartCount
	updated.FailureReason = reason
	updated.FinishTime = time.Time{}
	if err := m.TaskDb.Put(id, &updated); err != nil {
		log.Printf("Cannot mark task %s as failed: %v\n", id, err)
	}
}

// used when a restart relocates a task to a different
// worker so the previous container isn't orphaned.
// errors only logged
func (m *Manager) bestEffortStopOldContainer(addr string, t task.Task) {
	if t.ContainerID == "" {
		return
	}
	stopTask := t
	stopTask.State = task.Completed
	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
		Task:      stopTask,
	}
	data, err := json.Marshal(te)
	if err != nil {
		log.Printf("Error marshalling cleanup stop for task %s: %v", t.ID, err)
		return
	}
	url := httpclient.WorkerURL(addr, "/tasks")
	resp, err := httpclient.Worker().Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		log.Printf("Could not reach old owner %s to stop orphaned container %s: %v", addr, t.ContainerID, err)
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v", err)
		}
	}()
	if resp.StatusCode != http.StatusCreated {
		log.Printf("Old owner %s responded %d stopping orphaned container %s", addr, resp.StatusCode, t.ContainerID)
	}
}

func (m *Manager) stopTaskTerminal(t task.Task, reason string) {
	m.mu.Lock()
	w := readAssignment(m.Assignments, t.ID)
	t.State = task.Failed
	t.FailureReason = reason
	t.FinishTime = time.Now().UTC()
	if m.TaskDb == nil {
		log.Printf("Cannot store terminal task %s: task db is nil\n", t.ID)
	} else if err := m.TaskDb.Put(t.ID, &t); err != nil {
		log.Printf("Error storing terminal task %s: %v\n", t.ID, err)
	}
	m.mu.Unlock()

	m.Reservations.Release(w, &t)
	if w == "" {
		log.Printf("No worker owns terminal task %s; cannot send cleanup stop\n", t.ID)
		return
	}

	stopTask := t
	stopTask.State = task.Completed // will be Failed in the end since we sent over the failure reason
	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
		Task:      stopTask,
	}

	data, err := json.Marshal(te)
	if err != nil {
		log.Printf("Error marshalling task object: %v\n", t)
		return
	}

	url := httpclient.WorkerURL(w, "/tasks")
	resp, err := httpclient.Worker().Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		log.Printf("Error connecting to %v: %v", w, err)
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	if resp.StatusCode != http.StatusCreated {
		log.Printf("Error: worker %s responded with %d when stopping terminal task %s\n", w, resp.StatusCode, t.ID)
	}
}

// http/tcp health checks
func (m *Manager) checkTaskHealth(ctx context.Context, t task.Task, w string) error {
	hc := t.HealthCheck

	host, _, err := net.SplitHostPort(w)
	if err != nil {
		return fmt.Errorf("invalid worker address %q: %w", w, err)
	}
	hostPort := hostPortFor(t.HostPorts, hc.Port)
	if hostPort == 0 {
		return fmt.Errorf("error: task %s has no published host port for container port %d", t.ID, hc.Port)
	}

	switch hc.Type {
	case task.HealthCheckHTTP:
		path := hc.Path
		if path == "" {
			path = "/"
		}
		url := fmt.Sprintf("http://%s%s", net.JoinHostPort(host, strconv.Itoa(hostPort)), path)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("error building request: %w", err)
		}
		resp, err := httpclient.Plain().Do(req)
		if err != nil {
			return fmt.Errorf("error performing health check %s: %w", url, err)
		}
		defer func() {
			err = resp.Body.Close()
			if err != nil {
				log.Printf("Error closing response body: %v\n", err)
			}
		}()

		body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		if err != nil {
			return fmt.Errorf("error reading health check response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			output := strings.TrimSpace(string(body))
			if output != "" {
				return fmt.Errorf("health check %s returned %d: %s", url, resp.StatusCode, output)
			}

			return fmt.Errorf("health check %s returned %d", url, resp.StatusCode)
		}
		return nil

	case task.HealthCheckTCP:
		addr := net.JoinHostPort(host, strconv.Itoa(hostPort))

		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("tcp health check %s: %w", addr, err)
		}

		err = conn.Close()
		if err != nil {
			return fmt.Errorf("error closing tcp health check: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("unsupported manager-driven health check type %s", hc.Type)
	}
}

func (m *Manager) reconcileCheckers(ctx context.Context) {
	seen := make(map[uuid.UUID]struct{})

	for _, t := range m.getTasks() {
		seen[t.ID] = struct{}{}

		// only http/tcp health checks are managed by the manager
		managed := t.State == task.Running && t.HealthCheck != nil &&
			(t.HealthCheck.Type == task.HealthCheckHTTP || t.HealthCheck.Type == task.HealthCheckTCP)
		if !managed {
			m.stopChecker(t.ID)
			continue
		}

		if !m.checkers.has(t.ID) {
			m.startChecker(ctx, t)
			log.Printf("Started %s health checker for task %s\n", t.HealthCheck.Type, t.ID)
		}
	}

	m.checkers.stopAllExcept(seen)
}

func (m *Manager) restartFailedTasks() {
	for _, t := range m.getTasks() {
		if t.State != task.Failed {
			continue
		}

		if !task.ShouldRestart(t.RestartPolicy) {
			if t.FinishTime.IsZero() {
				reason := fmt.Sprintf("restart policy %q does not permit a restart", t.RestartPolicy)
				log.Printf("Task %s failed and its restart policy forbids a restart; stopping its container\n", t.ID)
				m.stopTaskTerminal(t, reason)
			}
			continue
		}

		restartCap := t.EffectiveMaxRestarts(MaxRestarts)
		if t.RestartCount < restartCap {
			err := m.restartTask(t)
			if err != nil {
				log.Printf("Error restarting task %s: %v", t.ID, err)
			}
			continue
		}

		if t.FinishTime.IsZero() {
			reason := t.FailureReason
			if reason == "" {
				reason = fmt.Sprintf("restart cap (%d) reached", restartCap)
			}
			log.Printf("Task %s reached its restart cap (%d); marking failed and stopping its container\n", t.ID, restartCap)
			m.stopTaskTerminal(t, reason)
		}
	}
}

func (m *Manager) startChecker(ctx context.Context, t task.Task) {
	// derive each per-task checker ctx from the manager's root ctx so a shutdown
	// (root cancel) tears down all running checkers, not just the ones
	// reconcileCheckers stops
	ctx, cancel := context.WithCancel(ctx)
	if !m.checkers.start(t.ID, cancel) {
		// a checker is already running; drop the fresh ctx to avoid a leak
		cancel()
		return
	}
	go m.runChecker(ctx, t)
}

func (m *Manager) stopChecker(id uuid.UUID) {
	m.checkers.stop(id)
}

func (m *Manager) runChecker(ctx context.Context, t task.Task) {
	hc := t.HealthCheck.Normalized()

	w := m.taskWorker(t.ID)
	if w == "" {
		log.Printf("Task %s is no longer assigned to a worker, stopping its checker\n", t.ID)
		return
	}

	if hc.StartPeriod > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(hc.StartPeriod) * time.Second):
		}
	}

	ticker := time.NewTicker(time.Duration(hc.Interval) * time.Second)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t, ok := m.getTask(t.ID)
			if !ok || t.State != task.Running {
				return
			}

			ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(hc.Timeout)*time.Second)
			err := m.checkTaskHealth(ctxTimeout, t, w)
			cancel() // normally i'd defer but this is in a constant for loop

			if err == nil {
				failures = 0
				continue
			}

			failures++
			log.Printf("Health check for task %s failed (%d/%d): %v\n", t.ID, failures, hc.Retries, err)
			if failures < hc.Retries {
				continue
			}

			if t.RestartCount >= t.EffectiveMaxRestarts(MaxRestarts) {
				reason := fmt.Sprintf("health check failed after all restarts & retries. last error: %v", err)
				log.Printf("Task %s: %s; marking failed and stopping its container\n", t.ID, reason)
				m.stopTaskTerminal(t, reason)
				return
			}

			if !task.ShouldRestart(t.RestartPolicy) {
				reason := fmt.Sprintf("health check failed (%v) and restart policy %q does not permit a restart", err, t.RestartPolicy)
				log.Printf("Task %s: %s; marking failed and stopping its container\n", t.ID, reason)
				m.stopTaskTerminal(t, reason)
				return
			}

			log.Printf("Task %s declared unhealthy after %d consecutive failures, restarting\n", t.ID, failures)
			err = m.restartTask(t)
			if err != nil {
				log.Printf("Error restarting task %s: %v", t.ID, err)
			}
			return
		}
	}
}

func (m *Manager) ProcessTasks(ctx context.Context) {
	ticker := time.NewTicker(LoopInterval)
	defer ticker.Stop()
	for {
		m.SendWork()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func hostPortFor(ports []task.PortMapping, containerPort int) int {
	for _, pm := range ports {
		if pm.ContainerPort == containerPort && pm.HostPort != 0 {
			return pm.HostPort
		}
	}
	return 0
}
