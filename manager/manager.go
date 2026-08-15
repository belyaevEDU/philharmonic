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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/belyaevedu/philharmonic/handlers"
	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/scheduler"
	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

const (
	WorkerTasksURL = "http://%s/tasks"
	WorkerRole     = "worker"

	MaxRestarts = 3

	LoopInterval = 10 * time.Second

	DbTasksFile   = "tasks.db"
	DbEventsFile  = "events.db"
	DbFilemode    = os.FileMode(0600)
	DbTaskBucket  = "tasks"
	DbEventBucket = "events"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        store.Store[task.Task]
	EventDb       store.Store[task.TaskEvent]
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int
	mu            sync.RWMutex
	pendingMu     sync.Mutex
	checkers      map[uuid.UUID]context.CancelFunc

	WorkerNodes []*node.Node
	Scheduler   scheduler.Scheduler
}

func New(workers []string, schedulerType, dbType string) (*Manager, error) {
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)

	var nodes []*node.Node
	for _, worker := range workers {
		workerTaskMap[worker] = []uuid.UUID{}

		nAPI := fmt.Sprintf("http://%v", worker)
		n, err := node.New(worker, nAPI, WorkerRole)
		if err != nil {
			return nil, fmt.Errorf("invalid worker %q: %w", worker, err)
		}
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case scheduler.RoundRobinDefaultName:
		s = scheduler.NewRoundRobin()
	case scheduler.EpvmDefaultName:
		s = scheduler.NewEpvm()
	default:
		s = scheduler.NewRoundRobin()
	}

	m := Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		checkers:      make(map[uuid.UUID]context.CancelFunc),
		WorkerNodes:   nodes,
		Scheduler:     s,
	}

	var ts store.Store[task.Task]
	var es store.Store[task.TaskEvent]
	var err error
	switch dbType {
	case store.MemoryType:
		ts = store.NewInMemoryStore[task.Task]()
		es = store.NewInMemoryStore[task.TaskEvent]()
	case store.BoltType:
		ts, err = store.NewBoltStore[task.Task](DbTasksFile, DbFilemode, DbTaskBucket)
		if err != nil {
			return nil, fmt.Errorf("opening tasks db: %w", err)
		}
		es, err = store.NewBoltStore[task.TaskEvent](DbEventsFile, DbFilemode, DbEventBucket)
		if err != nil {
			_ = ts.Close()
			return nil, fmt.Errorf("opening events db: %w", err)
		}
	default:
		return nil, errors.New("unknown db type given")
	}

	if err != nil {
		return nil, err
	}

	m.TaskDb = ts
	m.EventDb = es
	return &m, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	if m.TaskDb != nil {
		errs = append(errs, m.TaskDb.Close())
		m.TaskDb = nil
	}
	if m.EventDb != nil {
		errs = append(errs, m.EventDb.Close())
		m.EventDb = nil
	}
	return errors.Join(errs...)
}

func (m *Manager) SelectWorker(t *task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available candidates match resource requests for task %s", t.ID)
	}

	scores := m.Scheduler.Score(t, candidates)
	selectedNode := m.Scheduler.Pick(scores, candidates)
	if selectedNode == nil {
		return nil, fmt.Errorf("no candidate able to host task %s (all unscoreable)", t.ID)
	}

	return selectedNode, nil
}

func (m *Manager) AddTask(te task.TaskEvent) error {
	// A task ID identifies one task lifecycle
	if te.State != task.Completed && te.Task.State != task.Completed {
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

		queued := te.Task
		queued.State = task.Pending // updated, not yet sent to a worker
		if err := m.TaskDb.Put(queued.ID, &queued); err != nil {
			m.mu.Unlock()
			return fmt.Errorf("storing task %s: %w", queued.ID, err)
		}
		m.mu.Unlock()
	}

	m.pendingMu.Lock()
	m.Pending.Enqueue(te)
	m.pendingMu.Unlock()
	return nil
}

func (m *Manager) pendingLen() int {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	return m.Pending.Len()
}

func (m *Manager) dequeuePending() (task.TaskEvent, bool) {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()

	e := m.Pending.Dequeue()
	te, ok := e.(task.TaskEvent)
	if !ok {
		return task.TaskEvent{}, false
	}
	return te, true
}

func (m *Manager) enqueuePending(te task.TaskEvent) {
	m.pendingMu.Lock()
	m.Pending.Enqueue(te)
	m.pendingMu.Unlock()
}

// all snapshots now to not worry about race conditions
func (m *Manager) getTasks() []task.Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.TaskDb == nil {
		return []task.Task{}
	}

	persisted, err := m.TaskDb.List()
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

func (m *Manager) getTask(id uuid.UUID) (task.Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.TaskDb == nil {
		return task.Task{}, false
	}

	persisted, err := m.TaskDb.Get(id)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("Error getting task %s: %v\n", id, err)
		}
		return task.Task{}, false
	}
	return *persisted, true
}

func (m *Manager) taskWorker(id uuid.UUID) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.TaskWorkerMap[id]
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
	if m.pendingLen() > 0 {
		te, ok := m.dequeuePending()
		if !ok {
			log.Println("A non-task.TaskEvent object somehow got in the queue")
			return
		}

		t := te.Task
		isStop := te.State == task.Completed || t.State == task.Completed

		var w *node.Node
		owner := m.taskWorker(t.ID)
		if owner != "" {
			w = m.workerByAddress(owner)
			if w == nil {
				log.Printf("Cannot send task %s: assigned worker %q is unavailable\n", t.ID, owner)
				return
			}
		} else if isStop {
			log.Printf("Cannot stop task %s: no worker owns it\n", t.ID)
			return
		} else {
			var err error
			w, err = m.SelectWorker(&t)
			if err != nil {
				log.Printf("Error selecting worker for task %s: %v\n", t.ID, err)
				return
			}
		}

		if !isStop {
			t.State = task.Scheduled
			te.Task = t
			te.State = task.Scheduled
			if te.Timestamp.IsZero() {
				te.Timestamp = time.Now().UTC()
			}
		}
		log.Printf("Pulled %v off pending queue\n", t)

		data, err := json.Marshal(te)
		if err != nil {
			log.Printf("Error raised when marshalling task object %v: %v\n", t, err)
			return
		}

		url := fmt.Sprintf(WorkerTasksURL, w.Address)
		// ignoring gosec's G107 since the url is not from external input, but from an internal config
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
		if err != nil {
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

		m.mu.Lock()
		if m.EventDb == nil {
			log.Printf("Cannot store event %s: event db is nil\n", te.ID)
		} else if err := m.EventDb.Put(te.ID, &te); err != nil {
			log.Printf("Error storing event %s: %v\n", te.ID, err)
		}
		if !isStop {
			if m.TaskWorkerMap[t.ID] != w.Address {
				m.WorkerTaskMap[w.Address] = append(m.WorkerTaskMap[w.Address], t.ID)
			}
			m.TaskWorkerMap[t.ID] = w.Address
			if m.TaskDb == nil {
				log.Printf("Cannot store task %s: task db is nil\n", t.ID)
			} else if err := m.TaskDb.Put(t.ID, &t); err != nil {
				log.Printf("Error storing task %s: %v\n", t.ID, err)
			}
		}
		m.mu.Unlock()

		// the worker api returns the json of the newly created task
		// in response to POST /tasks
		t = task.Task{}
		err = d.Decode(&t)
		if err != nil {
			fmt.Printf("Error decoding response: %v\n", err)
			return
		}
		log.Printf("%#v\n", t) // # adds field names
	} else {
		log.Println("No work in the queue")
	}
}

func (m *Manager) fetchTasksFromWorker(worker string) ([]*task.Task, error) {
	url := fmt.Sprintf(WorkerTasksURL, worker)
	// ignoring gosec's G107 since the url is not from external input, but from an internal config
	resp, err := http.Get(url) // #nosec G107
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

func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		log.Printf("Checking worker %v for task updates\n", worker)

		tasks, err := m.fetchTasksFromWorker(worker)
		if err != nil {
			log.Printf("Error fetching tasks from %s: %v\n", worker, err)
			continue
		}

		for _, t := range tasks {
			log.Printf("Attempting to update task %s\n", t.ID.String())

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
				log.Printf(
					"Ignoring stale update for task %s: worker restart count %d, manager restart count %d\n",
					t.ID, t.RestartCount, persisted.RestartCount,
				)
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

			if err := m.TaskDb.Put(t.ID, &updated); err != nil {
				log.Printf("Error updating task %s: %v\n", t.ID, err)
			}
			m.mu.Unlock()
		}
	}
}

func (m *Manager) UpdateTasks(ctx context.Context) {
	ticker := time.NewTicker(LoopInterval)
	defer ticker.Stop()
	for {
		log.Println("[Manager] Checking for task updates from workers")
		m.updateTasks()
		log.Println("Task updates completed. Sleeping for 10 seconds")
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
		log.Println("[Manager] Reconciling task health checkers")
		m.reconcileCheckers(ctx)
		m.restartFailedTasks()
		log.Println("Sleeping for 10 seconds")
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

	var w *node.Node
	if owner != "" {
		w = m.workerByAddress(owner)
		if w == nil {
			reason := fmt.Sprintf("assigned worker %q is unavailable", owner)
			m.markFailed(t.ID, t.RestartCount+1, reason)
			return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
		}
	} else {
		var err error
		w, err = m.SelectWorker(&next)
		if err != nil {
			reason := fmt.Sprintf("no available worker to restart: %v", err)
			m.markFailed(t.ID, t.RestartCount+1, reason)
			return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
		}
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Scheduled,
		Timestamp: time.Now().UTC(),
		Task:      next,
	}

	data, err := json.Marshal(te)
	if err != nil {
		reason := fmt.Sprintf("unable to marshal restart event: %v", err)
		m.markFailed(t.ID, t.RestartCount+1, reason)
		return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
	}

	url := fmt.Sprintf(WorkerTasksURL, w.Address)
	// ignoring gosec's G107 since the url is not from external input, but from an internal config
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
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
	if m.TaskWorkerMap[t.ID] != w.Address {
		m.WorkerTaskMap[w.Address] = append(m.WorkerTaskMap[w.Address], t.ID)
	}
	m.TaskWorkerMap[t.ID] = w.Address
	if m.TaskDb == nil {
		m.mu.Unlock()
		return fmt.Errorf("cannot store restarted task %s: task db is nil", t.ID)
	}
	if err := m.TaskDb.Put(next.ID, &next); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("cannot store restarted task %s: %w", t.ID, err)
	}
	m.mu.Unlock()

	newTask := task.Task{}
	if err := d.Decode(&newTask); err != nil {
		log.Printf("Error decoding restart response: %v\n", err)
		// ownership already commited via 201, so we dont really care about a json decoding error
		return nil
	}

	log.Printf("Task restarted: %#v\n", newTask)
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

func (m *Manager) stopTaskTerminal(t task.Task, reason string) {
	m.mu.Lock()
	w := m.TaskWorkerMap[t.ID]
	t.State = task.Failed
	t.FailureReason = reason
	t.FinishTime = time.Now().UTC()
	if m.TaskDb == nil {
		log.Printf("Cannot store terminal task %s: task db is nil\n", t.ID)
	} else if err := m.TaskDb.Put(t.ID, &t); err != nil {
		log.Printf("Error storing terminal task %s: %v\n", t.ID, err)
	}
	m.mu.Unlock()

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

	url := fmt.Sprintf(WorkerTasksURL, w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
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
		resp, err := http.DefaultClient.Do(req)
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

		if _, running := m.checkers[t.ID]; !running {
			m.startChecker(ctx, t)
			log.Printf("Started %s health checker for task %s\n", t.HealthCheck.Type, t.ID)
		}
	}

	for id := range m.checkers {
		if _, ok := seen[id]; !ok {
			m.stopChecker(id)
		}
	}
}

func (m *Manager) restartFailedTasks() {
	for _, t := range m.getTasks() {
		if t.State != task.Failed {
			continue
		}

		if t.RestartCount < MaxRestarts {
			err := m.restartTask(t)
			if err != nil {
				log.Printf("Error restarting task %s: %v", t.ID, err)
			}
			continue
		}

		if t.FinishTime.IsZero() {
			reason := t.FailureReason
			if reason == "" {
				reason = fmt.Sprintf("restart cap (%d) reached", MaxRestarts)
			}
			log.Printf("Task %s reached the restart cap (%d); marking failed and stopping its container\n", t.ID, MaxRestarts)
			m.stopTaskTerminal(t, reason)
		}
	}
}

func (m *Manager) startChecker(ctx context.Context, t task.Task) {
	// derive each per-task checker ctx from the manager's root ctx so a shutdown
	// (root cancel) tears down all running checkers, not just the ones
	// reconcileCheckers stops
	ctx, cancel := context.WithCancel(ctx)
	m.checkers[t.ID] = cancel
	go m.runChecker(ctx, t)
}

func (m *Manager) stopChecker(id uuid.UUID) {
	if cancel, ok := m.checkers[id]; ok {
		cancel()
		delete(m.checkers, id)
	}
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

			if t.RestartCount >= MaxRestarts {
				reason := fmt.Sprintf("health check failed after all restarts & retries. last error: %v", err)
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
		log.Println("[Manager] Processing any tasks in the queue")
		m.SendWork()
		log.Println("Sleeping for 10 seconds")
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
