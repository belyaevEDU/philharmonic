package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/belyaevedu/philharmonic/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
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

func (m *Manager) UpdateTasks() {
	// updates tasks
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

		url := fmt.Sprintf("http://%s/tasks", w)
		// ignoring gosec's G107 since the url is not from external input, but from an internal config
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
		if err != nil {
			fmt.Printf("Error connecting to %v: %v\n", w, err)
			m.Pending.Enqueue(te)
			return
		}

		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {
			hr := worker.HTTPResponse{}
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
