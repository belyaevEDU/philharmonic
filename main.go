package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/belyaevedu/philharmonic/manager"
	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/worker"
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
	w1, err := worker.New("", store.MemoryType)
	if err != nil {
		fmt.Printf("error creating worker: %v\n", err)
		return
	}
	w2, err := worker.New("", store.MemoryType)
	if err != nil {
		fmt.Printf("error creating worker: %v\n", err)
		return
	}
	w3, err := worker.New("", store.MemoryType)
	if err != nil {
		fmt.Printf("error creating worker: %v\n", err)
		return
	}
	wapi1 := worker.Api{Address: whost, Port: wport, Worker: w1}
	wapi2 := worker.Api{Address: whost, Port: wport + 1, Worker: w2}
	wapi3 := worker.Api{Address: whost, Port: wport + 2, Worker: w3}

	for _, w := range []*worker.Worker{w1, w2, w3} {
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
		net.JoinHostPort(whost, strconv.Itoa(wport)),
		net.JoinHostPort(whost, strconv.Itoa(wport+1)),
		net.JoinHostPort(whost, strconv.Itoa(wport+2)),
	}
	m, err := manager.New(workers, "", store.MemoryType)
	if err != nil {
		fmt.Printf("invalid worker configuration: %v\n", err)
		return
	}
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
