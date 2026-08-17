package manager

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/go-chi/chi/v5"
)

const (
	apiReadHeaderTimeout = 5 * time.Second
	apiShutdownTimeout   = 10 * time.Second
)

type Api struct {
	Address string
	Port    int
	Manager *Manager
	Router  *chi.Mux

	server *http.Server
}

type HTTPResponse struct {
	HTTPStatusCode int
	Message        string
}

type TaskView struct {
	task.Task
	Worker string `json:",omitempty"`
}

type NodeView struct {
	node.Snapshot
	Address string `json:",omitempty"`
	Role    string `json:",omitempty"`
}

func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Route("/tasks", func(r chi.Router) {
		r.Post("/", a.StartTaskHandler)
		r.Get("/", a.GetTasksHandler)
		r.Route("/{taskID}", func(r chi.Router) {
			r.Delete("/", a.StopTaskHandler)
		})
	})
	a.Router.Route("/nodes", func(r chi.Router) {
		r.Get("/", a.GetNodesHandler)
	})
}

func (a *Api) Start() error {
	a.initRouter()

	a.server = &http.Server{
		Addr:              net.JoinHostPort(a.Address, strconv.Itoa(a.Port)),
		Handler:           a.Router,
		ReadHeaderTimeout: apiReadHeaderTimeout,
	}

	err := a.server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("error raised when starting an http server: %w", err)
	}
	return nil
}

func (a *Api) Shutdown() error {
	if a.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), apiShutdownTimeout)
	defer cancel()
	return a.server.Shutdown(ctx)
}
