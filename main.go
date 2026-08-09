package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/belyaevedu/philharmonic/manager"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/belyaevedu/philharmonic/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

func main() {
	host := os.Getenv("CUBE_HOST")
	port, err := strconv.Atoi(os.Getenv("CUBE_PORT"))
	if err != nil {
		fmt.Printf("port string->int conversion error: %v", err)
		return
	}

	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	api := worker.Api{Address: host, Port: port, Worker: &w}

	go runTasks(&w)
	go w.CollectStats()
	go func() {
		err = api.Start()
		if err != nil {
			fmt.Printf("Error raised when starting the http server: %v", err)
			os.Exit(1)
		}
	}()

	// api.Start()
	time.Sleep(3 * time.Second)

	workers := []string{fmt.Sprintf("%s:%d", host, port)}
	m := manager.New(workers)

	for i := 0; i < 3; i++ {
		t := task.Task{
			ID:    uuid.New(),
			Name:  fmt.Sprintf("test-container-%d", i),
			State: task.Scheduled,
			Image: "strm/helloworld-http",
		}
		te := task.TaskEvent{
			ID:    uuid.New(),
			State: task.Running,
			Task:  t,
		}
		m.AddTask(te)
		m.SendTask()
	}

	go func() {
		for {
			fmt.Printf("[Manager] Updating tasks from %d workers\n", len(m.Workers))
			m.UpdateTasks()

			time.Sleep(10 * time.Second)
		}
	}()

	for {
		for _, t := range m.TaskDb {
			fmt.Printf("[Manager] Task: id %s, state %d\n", t.ID, t.State)
			time.Sleep(10 * time.Second)
		}
	}
}

func runTasks(w *worker.Worker) {
	for {
		if w.Queue.Len() != 0 {
			result := w.RunTask()
			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Println("No tasks to process currently.")
		}
		time.Sleep(time.Second * 10)
	}
}
