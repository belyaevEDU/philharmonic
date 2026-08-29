package manager

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

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
	//   hold the mutex across their store I/O
	// - pure readers snapshot the store pointer under the mutex, release it,
	//   and never hold the mutex during store I/O to not stall other processes
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
				n.ClearStats()
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
