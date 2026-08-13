package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/belyaevedu/philharmonic/handlers"
	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/scheduler"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/belyaevedu/philharmonic/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

const (
	WorkerTasksURL = "http://%s/tasks"
	WorkerRole     = "worker"

	MaxRestarts = 3
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[uuid.UUID]*task.Task
	EventDb       map[uuid.UUID]*task.TaskEvent
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

func New(workers []string, schedulerType string) *Manager {
	taskDb := make(map[uuid.UUID]*task.Task)
	eventDb := make(map[uuid.UUID]*task.TaskEvent)
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)

	var nodes []*node.Node
	for _, worker := range workers {
		workerTaskMap[worker] = []uuid.UUID{}

		nAPI := fmt.Sprintf("http://%v", worker)
		n := node.NewNode(worker, nAPI, WorkerRole)
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case scheduler.RoundRobinDefaultName:
		s = scheduler.NewRoundRobin()
	default:
		s = scheduler.NewRoundRobin()
	}

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        taskDb,
		EventDb:       eventDb,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		checkers:      make(map[uuid.UUID]context.CancelFunc),
		WorkerNodes:   nodes,
		Scheduler:     s,
	}
}

func (m *Manager) SelectWorker(t *task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		return nil, fmt.Errorf("no available candidates match resource requests for task %s", t.ID)
	}

	scores := m.Scheduler.Score(t, candidates)
	selectedNode := m.Scheduler.Pick(scores, candidates)

	return selectedNode, nil
}

func (m *Manager) AddTask(te task.TaskEvent) error {
	// A task ID identifies one task lifecycle
	if te.State != task.Completed && te.Task.State != task.Completed {
		m.mu.Lock()
		if m.TaskDb == nil {
			m.TaskDb = make(map[uuid.UUID]*task.Task)
		}
		if _, exists := m.TaskDb[te.Task.ID]; exists {
			m.mu.Unlock()
			return fmt.Errorf("task %s already exists", te.Task.ID)
		}

		queued := te.Task
		queued.State = task.Pending // updated, not yet sent to a worker
		m.TaskDb[queued.ID] = &queued
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

	tasks := make([]task.Task, 0, len(m.TaskDb))
	for _, persisted := range m.TaskDb {
		tasks = append(tasks, *persisted)
	}
	return tasks
}

func (m *Manager) getTask(id uuid.UUID) (task.Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	persisted, ok := m.TaskDb[id]
	if !ok {
		return task.Task{}, false
	}
	return *persisted, true
}

func (m *Manager) taskWorker(id uuid.UUID) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.TaskWorkerMap[id]
}

func (m *Manager) workerByName(name string) *node.Node {
	for _, worker := range m.WorkerNodes {
		if worker.Name == name {
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
			w = m.workerByName(owner)
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

		m.mu.Lock()
		m.EventDb[te.ID] = &te
		if !isStop {
			m.WorkerTaskMap[w.Name] = append(m.WorkerTaskMap[w.Name], t.ID)
			m.TaskWorkerMap[t.ID] = w.Name
			m.TaskDb[t.ID] = &t
		}
		m.mu.Unlock()

		data, err := json.Marshal(te)
		if err != nil {
			log.Printf("Error raised when marshalling task object %v: %v\n", t, err)
			return
		}

		url := fmt.Sprintf(WorkerTasksURL, w.Name)
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
			err := d.Decode(&hr)
			if err != nil {
				fmt.Printf("Error decoding response: %v\n", err)
				return
			}

			log.Printf("Response error (%d): %s", hr.HTTPStatusCode, hr.Message)
			return
		}

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
			persisted, ok := m.TaskDb[t.ID]
			if !ok {
				m.mu.Unlock()
				log.Printf("Task with ID %s not found\n", t.ID.String())
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

			persisted.ContainerID = t.ContainerID
			persisted.HostPorts = t.HostPorts
			if !t.StartTime.IsZero() {
				persisted.StartTime = t.StartTime
			}
			if !t.FinishTime.IsZero() {
				persisted.FinishTime = t.FinishTime
			}

			if t.State == task.Failed {
				persisted.State = task.Failed
				if t.FailureReason != "" {
					persisted.FailureReason = t.FailureReason
				}
			} else if persisted.State != task.Failed || persisted.FinishTime.IsZero() {
				// not terminal-failed -> trust the worker's state
				persisted.State = t.State
			}
			// else: terminal-failed here

			m.mu.Unlock()
		}
	}
}

func (m *Manager) UpdateTasks() {
	for {
		log.Println("[Manager] Checking for task updates from workers")
		m.updateTasks()
		log.Println("Task updates completed. Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) DoHealthChecks() {
	for {
		log.Println("[Manager] Reconciling task health checkers")
		m.reconcileCheckers()
		m.restartFailedTasks()
		log.Println("Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) restartTask(t task.Task) error {
	m.mu.Lock()
	w := m.TaskWorkerMap[t.ID]
	t.State = task.Scheduled
	t.RestartCount++
	t.FailureReason = ""
	t.StartTime = time.Time{}
	t.FinishTime = time.Time{}
	m.TaskDb[t.ID] = &t
	m.mu.Unlock()

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Scheduled,
		Timestamp: time.Now().UTC(),
		Task:      t,
	}

	data, err := json.Marshal(te)
	if err != nil {
		return fmt.Errorf("unable to marshal task object %s: %w", t.ID, err)
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		m.enqueuePending(te)
		return fmt.Errorf("error POSTing to %s: %w", w, err)
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %v", err)
		}
	}()

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		hr := worker.HTTPResponse{}
		err := d.Decode(&hr)
		if err != nil {
			return fmt.Errorf("error decoding response: %w", err)
		}
		return fmt.Errorf("response error (%d): %s", hr.HTTPStatusCode, hr.Message)
	}

	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		return fmt.Errorf("error decoding response: %w", err)
	}

	log.Printf("Task restarted: %#v\n", newTask)
	return nil
}

func (m *Manager) stopTaskTerminal(t task.Task, reason string) {
	m.mu.Lock()
	w := m.TaskWorkerMap[t.ID]
	t.State = task.Failed
	t.FailureReason = reason
	t.FinishTime = time.Now().UTC()
	m.TaskDb[t.ID] = &t
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

	url := fmt.Sprintf("http://%s/tasks", w)
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

	host := strings.SplitN(w, ":", 2)[0]
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
		url := fmt.Sprintf("http://%s:%d%s", host, hostPort, path)

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
		addr := fmt.Sprintf("%s:%d", host, hostPort)

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

func (m *Manager) reconcileCheckers() {
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
			m.startChecker(t)
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

func (m *Manager) startChecker(t task.Task) {
	ctx, cancel := context.WithCancel(context.Background())
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

func (m *Manager) ProcessTasks() {
	for {
		log.Println("[Manager] Processing any tasks in the queue")
		m.SendWork()
		log.Println("Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
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
