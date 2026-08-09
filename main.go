package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

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

	fmt.Println("starting worker")

	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	api := worker.Api{Address: host, Port: port, Worker: &w}

	go runTasks(&w)
	go w.CollectStats()
	err = api.Start()
	if err != nil {
		fmt.Printf("Error raised when starting the http server: %v", err)
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
			log.Printf("No tasks to process currently.\n")
		}
		log.Println("sleeping")
		time.Sleep(time.Second * 10)
	}
}
