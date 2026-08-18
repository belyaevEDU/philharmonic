package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

const (
	apiReadHeaderTimeout = 5 * time.Second
)

var ApiShutdownTimeout = 10 * time.Second

type Api struct {
	Address string
	Port    int
	Worker  *Worker
	Router  *chi.Mux

	server *http.Server
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

	a.Router.Route("/stats", func(r chi.Router) {
		r.Get("/", a.GetStatsHandler)
	})

	a.Router.Route("/ports", func(r chi.Router) {
		r.Get("/", a.GetPortsHandler)
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
	ctx, cancel := context.WithTimeout(context.Background(), ApiShutdownTimeout)
	defer cancel()
	return a.server.Shutdown(ctx)
}
