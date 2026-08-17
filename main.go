package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/belyaevedu/philharmonic/manager"
	"github.com/belyaevedu/philharmonic/scheduler"
	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/worker"
)

const (
	startupSettle = 2 * time.Second
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("starting workers")
	workers := make([]*worker.Worker, 0, 3)
	closeWorkers := func() {
		for _, w := range workers {
			if w != nil {
				_ = w.Close()
			}
		}
	}

	for i := 0; i < 3; i++ {
		w, err := worker.New(scheduler.EpvmDefaultName, store.BoltType)
		if err != nil {
			fmt.Printf("error creating worker: %v\n", err)
			closeWorkers()
			return
		}
		workers = append(workers, w)
	}
	workerApis := []*worker.Api{
		{Address: whost, Port: wport, Worker: workers[0]},
		{Address: whost, Port: wport + 1, Worker: workers[1]},
		{Address: whost, Port: wport + 2, Worker: workers[2]},
	}
	shutdownWorkerApis := func() {
		for _, api := range workerApis {
			if err := api.Shutdown(); err != nil {
				fmt.Printf("error shutting down worker api: %v\n", err)
			}
		}
	}

	for _, w := range workers {
		go w.RunTasks(ctx)
		go w.CollectStats(ctx)
		go w.UpdateTasks(ctx)
	}

	for _, api := range workerApis {
		go func(api *worker.Api) {
			if err := api.Start(); err != nil {
				fmt.Printf("Error raised when starting the http server: %v", err)
				closeWorkers()
				os.Exit(1)
			}
		}(api)
	}

	time.Sleep(startupSettle)

	fmt.Println("Starting manager")

	workerAddrs := []string{
		net.JoinHostPort(whost, strconv.Itoa(wport)),
		net.JoinHostPort(whost, strconv.Itoa(wport+1)),
		net.JoinHostPort(whost, strconv.Itoa(wport+2)),
	}
	m, err := manager.New(workerAddrs, "", store.MemoryType)
	if err != nil {
		fmt.Printf("invalid worker configuration: %v\n", err)
		shutdownWorkerApis()
		closeWorkers()
		return
	}
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks(ctx)
	go m.UpdateTasks(ctx)
	go m.DoHealthChecks(ctx)
	go m.RefreshNodeStats(ctx)

	go func() {
		<-ctx.Done()
		fmt.Println("\nshutting down...")
		shutdownWorkerApis()
		if err := mapi.Shutdown(); err != nil {
			fmt.Printf("error shutting down manager api: %v\n", err)
		}
	}()

	err = mapi.Start()
	if err != nil {
		fmt.Printf("Error raised when starting the http server: %v\n", err)
	}

	if cerr := m.Close(); cerr != nil {
		fmt.Printf("error closing manager stores: %v\n", cerr)
	}
	closeWorkers()

	if err != nil {
		os.Exit(1)
	}
}
