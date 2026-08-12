package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/belyaevedu/philharmonic/handlers"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/belyaevedu/philharmonic/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

const (
	WorkerTasksURL = "http://%s/tasks"

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
	checkers      map[uuid.UUID]context.CancelFunc
}

func New(workers []string) *Manager {
	taskDb := make(map[uuid.UUID]*task.Task)
	eventDb := make(map[uuid.UUID]*task.TaskEvent)
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)

	for _, worker := range workers {
		workerTaskMap[worker] = []uuid.UUID{}
	}

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        taskDb,
		EventDb:       eventDb,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		checkers:      make(map[uuid.UUID]context.CancelFunc),
	}
}

func (m *Manager) SelectWorker() string {
	// naive round-robin
	// e-pvm later

	var newWorker int
	if m.LastWorker+1 < len(m.Workers) {
		newWorker = m.LastWorker + 1
	} else {
		m.LastWorker = 0
	}
	m.LastWorker = newWorker

	return m.Workers[newWorker]
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

func (m *Manager) getTasks() []*task.Task {
	tasks := slices.Collect(maps.Values(m.TaskDb))
	if tasks == nil {
		return []*task.Task{}
	}

	return tasks
}

func (m *Manager) SendWork() {
	if m.Pending.Len() > 0 {
		w := m.SelectWorker()
		e := m.Pending.Dequeue()
		te, ok := e.(task.TaskEvent)
		if !ok {
			log.Printf("A non-task.TaskEvent object somehow got in the queue: %v\n", e)
			return
		}

		t := te.Task
		log.Printf("Pulled %v off pending queue\n", t)

		m.EventDb[te.ID] = &te
		m.WorkerTaskMap[w] = append(m.WorkerTaskMap[w], t.ID)
		m.TaskWorkerMap[t.ID] = w

		t.State = task.Scheduled
		m.TaskDb[t.ID] = &t

		data, err := json.Marshal(te)
		if err != nil {
			log.Printf("Error raised when marshalling task object %v: %v\n", t, err)
		}

		url := fmt.Sprintf(WorkerTasksURL, w)
		// ignoring gosec's G107 since the url is not from external input, but from an internal config
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
		if err != nil {
			fmt.Printf("Error connecting to %v: %v\n", w, err)
			m.Pending.Enqueue(te)
			return
		}

		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {
			hr := handlers.HTTPResponse{}
			err := d.Decode(&e)
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

func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		log.Printf("Checking worker %v for task updates\n", worker)

		url := fmt.Sprintf(WorkerTasksURL, worker)
		// ignoring gosec's G107 since the url is not from external input, but from an internal config
		resp, err := http.Get(url) // #nosec G107
		if err != nil {
			log.Printf("Error connecting to %s: %v\n", worker, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Error sending request to worker %s: %v\n", worker, err)
			continue
		}

		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = d.Decode(&tasks)
		if err != nil {
			log.Printf("Error unmarshalling tasks from %s: %v", worker, err)
			continue
		}

		for _, t := range tasks {
			log.Printf("Attempting to update task %s\n", t.ID.String())

			_, ok := m.TaskDb[t.ID]
			if !ok {
				log.Printf("Task with ID %s not found\n", t.ID.String())
				return
			}

			m.TaskDb[t.ID].State = t.State
			m.TaskDb[t.ID].StartTime = t.StartTime
			m.TaskDb[t.ID].FinishTime = t.FinishTime
			m.TaskDb[t.ID].ContainerID = t.ContainerID
			m.TaskDb[t.ID].HostPorts = t.HostPorts
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
		log.Println("Performing task health check")
		m.doHealthChecks()
		log.Println("Task health checks completed")
		log.Println("Sleeping for 60 seconds")
		time.Sleep(60 * time.Second) // this is in dire need of a rewrite
	}
}

func (m *Manager) restartTask(t *task.Task) {
	w := m.TaskWorkerMap[t.ID]
	t.State = task.Scheduled
	t.RestartCount++

	m.TaskDb[t.ID] = t

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}

	data, err := json.Marshal(te)
	if err != nil {
		// just rewrite it to return the errors
		// fuck you tim boring
		log.Printf("Unable to marshal task object: %v\n", t)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		log.Printf("Error connecting to %v: %v", w, err)
		m.Pending.Enqueue(te)
		return
	}

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		hr := worker.HTTPResponse{}
		err := d.Decode(&hr)
		if err != nil {
			fmt.Printf("Error decoding response: %v\n", err)
			return
		}
		log.Printf("Response error (%d): %s", hr.HTTPStatusCode, hr.Message)
		return
	}

	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		fmt.Printf("Error decoding response: %v\n", err)
		return
	}

	// tim boring wanted to output the variable 't' here
	// the fuck is that for????
	// just an abundance of terrible design choices i'll have to overcome
	// some of them i've already refactored
	// a learning experience some might say
	log.Printf("%#v\n", newTask)
}

// http/tcp health checks
func (m *Manager) checkTaskHealth(ctx context.Context, t *task.Task, w string) error {
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
		defer resp.Body.Close()

		_, err = io.Copy(io.Discard, resp.Body)
		if err != nil {
			return fmt.Errorf("error copying to discard somehow?: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
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
		if t.State == task.Failed && t.RestartCount < MaxRestarts {
			m.restartTask(t)
		}
	}
}

func (m *Manager) startChecker(t *task.Task) {
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

func (m *Manager) runChecker(ctx context.Context, t *task.Task) {
	hc := t.HealthCheck.Normalized()

	m.mu.RLock()
	w := m.TaskWorkerMap[t.ID]
	m.mu.RUnlock()
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
			if t.State != task.Running {
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
				log.Printf(
					"Task %s keeps failing its health check but reached the restart cap (%d); giving up\n",
					t.ID, MaxRestarts,
				)
				return
			}

			log.Printf("Task %s declared unhealthy after %d consecutive failures, restarting\n", t.ID, failures)
			m.restartTask(t)
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
