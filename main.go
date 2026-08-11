package main

import (
	"fmt"
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
	whost := os.Getenv("CUBE_WORKER_HOST")
	wport, err := strconv.Atoi(os.Getenv("CUBE_WORKER_PORT"))
	if err != nil {
		fmt.Printf("port string->int conversion error: %v", err)
		return
	}

	mhost := os.Getenv("CUBE_MANAGER_HOST")
	mport, err := strconv.Atoi(os.Getenv("CUBE_MANAGER_PORT"))
	if err != nil {
		fmt.Printf("port string->int conversion error: %v", err)
		return
	}

	fmt.Println("starting worker")
	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	api := worker.Api{Address: whost, Port: wport, Worker: &w}

	go w.RunTasks()
	go w.CollectStats()
	go func() {
		err = api.Start()
		if err != nil {
			fmt.Printf("Error raised when starting the http server: %v", err)
			os.Exit(1)
		}
	}()

	time.Sleep(2 * time.Second)

	fmt.Println("Starting manager")

	workers := []string{fmt.Sprintf("%s:%d", whost, wport)}
	m := manager.New(workers)
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()
	err = mapi.Start()
	if err != nil {
		fmt.Printf("Error raised when starting the http server: %v", err)
		os.Exit(1)
	}
}
