package manager

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

type Api struct {
	Address string
	Port    int
	Manager *Manager
	Router  *chi.Mux
}

type HTTPResponse struct {
	HTTPStatusCode int
	Message        string
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
}

func (a *Api) Start() error {
	a.initRouter()

	server := http.Server{
		Addr:              fmt.Sprintf("%s:%d", a.Address, a.Port),
		Handler:           a.Router,
		ReadHeaderTimeout: time.Second * 5,
	}

	err := server.ListenAndServe()
	if err != nil {
		return fmt.Errorf("error raised when starting an http server: %w", err)
	}
	return nil
}
