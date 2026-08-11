package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/belyaevedu/philharmonic/handlers"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/belyaevedu/philharmonic/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/network"
)

const (
	WorkerTasksURL = "http://%s/tasks"

	HealthCheckFailCap = 3
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[uuid.UUID]*task.Task
	EventDb       map[uuid.UUID]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int
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
		m.Pending.Enqueue(t)
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

func (m *Manager) checkTaskHealth(t task.Task) error {
	log.Printf("Calling health check for task %s: %s\n", t.ID, t.HealthCheck)

	w, ok := m.TaskWorkerMap[t.ID]
	if !ok {
		return errors.New("error raised checking task health: task not in TaskWorkerMap")
	}

	hostPort := getHostPort(t.HostPorts)
	worker := strings.Split(w, ":")
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)

	log.Printf("Calling health check for task %s: %s\n", t.ID, url)

	resp, err := http.Get(url) // #nosec G107
	if err != nil {
		msg := fmt.Sprintf("error connecting to health check %s", url)
		log.Println(msg)
		return errors.New(msg)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("error: health check for task %s did not return 200", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	log.Printf("Task %s health check response: %v\n", t.ID, resp.StatusCode)

	return nil
}

func (m *Manager) doHealthChecks() {
	for _, t := range m.getTasks() {
		if t.State == task.Running && t.RestartCount < HealthCheckFailCap {
			err := m.checkTaskHealth(*t)
			// i'd nuke the log.printlns for every error out of there and log it here
			if err != nil {
				if t.RestartCount < HealthCheckFailCap {
					m.restartTask(t)
				}
			}
		} else if t.State == task.Failed && t.RestartCount < HealthCheckFailCap {
			m.restartTask(t)
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

func getHostPort(ports network.PortMap) *string {
	for k := range ports {
		return &ports[k][0].HostPort // atrocious
	}
	return nil
}
