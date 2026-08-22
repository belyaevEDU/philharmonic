package manager

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/belyaevedu/philharmonic/auth"
	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/go-chi/chi/v5"
)

const (
	apiReadHeaderTimeout = 5 * time.Second
)

var ApiShutdownTimeout = 10 * time.Second

type Api struct {
	Address string
	Port    int
	Manager *Manager
	Router  *chi.Mux

	// enables HTTPS when non-nil
	// a non-nil config with ClientCAs set additionally requires
	// callers to authenticate with a cluster certificate via mTLS
	TLSConfig *tls.Config

	// auth enables bearer-token user authentication when non-nil
	// When nil, the API accepts unauthenticated requests
	Auth *auth.TokenStore

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

	viewer := func(h http.HandlerFunc) http.HandlerFunc { return h }
	admin := viewer
	if a.Auth != nil {
		viewer = func(h http.HandlerFunc) http.HandlerFunc {
			return auth.RequireRoleHandler(auth.RoleViewer, h)
		}
		admin = func(h http.HandlerFunc) http.HandlerFunc {
			return auth.RequireRoleHandler(auth.RoleAdmin, h)
		}

		a.Router.Use(auth.BearerAuth(a.Auth))
	}

	a.Router.Route("/tasks", func(r chi.Router) {
		r.Get("/", viewer(a.GetTasksHandler))
		r.Post("/", admin(a.StartTaskHandler))
		r.Get("/logs/{taskID}", viewer(a.GetTaskLogsHandler))
		r.Route("/{taskID}", func(r chi.Router) {
			r.Delete("/", admin(a.StopTaskHandler))
		})
	})
	a.Router.Route("/nodes", func(r chi.Router) {
		r.Get("/", viewer(a.GetNodesHandler))
	})
	a.Router.Route("/images", func(r chi.Router) {
		r.Post("/", admin(a.PullImageHandler))
	})
}

func (a *Api) Start() error {
	a.initRouter()

	a.server = &http.Server{
		Addr:              net.JoinHostPort(a.Address, strconv.Itoa(a.Port)),
		Handler:           a.Router,
		ReadHeaderTimeout: apiReadHeaderTimeout,
	}

	var err error
	if a.TLSConfig != nil {
		a.server.TLSConfig = a.TLSConfig
		err = a.server.ListenAndServeTLS("", "")
	} else {
		err = a.server.ListenAndServe()
	}
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
