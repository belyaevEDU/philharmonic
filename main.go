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

	fmt.Println("starting workers")
	w1 := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi1 := worker.Api{Address: whost, Port: wport, Worker: &w1}

	w2 := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi2 := worker.Api{Address: whost, Port: wport + 1, Worker: &w2}

	w3 := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi3 := worker.Api{Address: whost, Port: wport + 2, Worker: &w3}

	for _, w := range []*worker.Worker{&w1, &w2, &w3} {
		go w.RunTasks()
		go w.CollectStats()
		go w.UpdateTasks()
	}

	for _, api := range []*worker.Api{&wapi1, &wapi2, &wapi3} {
		go func(api *worker.Api) {
			err := api.Start()
			if err != nil {
				fmt.Printf("Error raised when starting the http server: %v", err)
				os.Exit(1)
			}
		}(api)
	}

	time.Sleep(2 * time.Second)

	fmt.Println("Starting manager")

	workers := []string{
		fmt.Sprintf("%s:%d", whost, wport),
		fmt.Sprintf("%s:%d", whost, wport+1),
		fmt.Sprintf("%s:%d", whost, wport+2),
	}
	m := manager.New(workers, "")
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
