package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"net/http"
	"slices"

	"github.com/belyaevedu/philharmonic/handlers"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

const (
	WorkerTasksURL = "http://%s/tasks"
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

func (m *Manager) UpdateTasks() {
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

func (m *Manager) SendTask() {
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
