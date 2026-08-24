package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/belyaevedu/philharmonic/handlers"
	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.TaskEvent{}
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf(handlers.ErrorUnmarshallingJson, err)
		err = handlers.HttpResponseHelper(w, msg, http.StatusBadRequest)
		if err != nil {
			log.Printf(handlers.ErrorEncodingJson, err.Error())
		}
		return
	}

	if te.State != task.Completed && te.Task.State != task.Completed {
		if err := task.ValidatePortMappings(te.Task.Ports); err != nil {
			if responseErr := handlers.HttpResponseHelper(w, err.Error(), http.StatusBadRequest); responseErr != nil {
				log.Printf(handlers.ErrorEncodingJson, responseErr)
			}
			return
		}
	}

	if err := a.Worker.AddTask(te.Task); err != nil {
		if responseErr := handlers.HttpResponseHelper(w, err.Error(), http.StatusInternalServerError); responseErr != nil {
			log.Printf(handlers.ErrorEncodingJson, responseErr)
		}
		return
	}
	log.Printf("Added task %v\n", te.Task.ID)
	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(te.Task)
	if err != nil {
		log.Printf(handlers.ErrorEncodingJsonWithTaskID, te.Task.ID.String(), err.Error())
	}
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(a.Worker.getTasks())
	if err != nil {
		log.Printf(handlers.ErrorEncodingJson, err.Error())
	}
}

func (a *Api) ForgetTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Println("No taskID passed in the request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	tID, err := uuid.Parse(taskID)
	if err != nil {
		log.Println("Non-UUID taskID passed in the request.")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := a.Worker.ForgetTask(tID); err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			w.WriteHeader(http.StatusNotFound)
		case errors.Is(err, errTaskStillActive):
			if responseErr := handlers.HttpResponseHelper(w, err.Error(), http.StatusConflict); responseErr != nil {
				log.Printf(handlers.ErrorEncodingJson, responseErr)
			}
		default:
			if responseErr := handlers.HttpResponseHelper(w, err.Error(), http.StatusInternalServerError); responseErr != nil {
				log.Printf(handlers.ErrorEncodingJson, responseErr)
			}
		}
		return
	}

	log.Printf("Forgot task %v\n", tID)
	w.WriteHeader(http.StatusNoContent)
}

func (a *Api) GetStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	a.Worker.statsMu.RLock()
	stats := a.Worker.Stats
	a.Worker.statsMu.RUnlock()

	err := json.NewEncoder(w).Encode(stats)
	if err != nil {
		log.Printf(handlers.ErrorEncodingJson, err)
	}
}

func (a *Api) GetPortsHandler(w http.ResponseWriter, r *http.Request) {
	ports, err := a.Worker.HostPortsWithError()
	if err != nil {
		msg := fmt.Sprintf("error reading occupied host ports: %v", err)
		if responseErr := handlers.HttpResponseHelper(w, msg, http.StatusInternalServerError); responseErr != nil {
			log.Printf(handlers.ErrorEncodingJson, responseErr)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(ports); err != nil {
		log.Printf(handlers.ErrorEncodingJson, err)
	}
}

// pulls the requested image on this worker's docker daemon.
// the pull is synchronous and can take a while for large images.
// callers should use a bigger client timeout than default
func (a *Api) PullImageHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	req := PullImageRequest{}
	err := d.Decode(&req)
	if err != nil {
		msg := fmt.Sprintf(handlers.ErrorUnmarshallingJson, err)
		if responseErr := handlers.HttpResponseHelper(w, msg, http.StatusBadRequest); responseErr != nil {
			log.Printf(handlers.ErrorEncodingJson, responseErr)
		}
		return
	}

	image := strings.TrimSpace(req.Image)
	if image == "" {
		msg := "field \"image\" is required"
		if responseErr := handlers.HttpResponseHelper(w, msg, http.StatusBadRequest); responseErr != nil {
			log.Printf(handlers.ErrorEncodingJson, responseErr)
		}
		return
	}

	pulled, err := a.Worker.PullImage(image)
	if err != nil {
		msg := fmt.Sprintf("error pulling image %q: %v", image, err)
		if responseErr := handlers.HttpResponseHelper(w, msg, http.StatusInternalServerError); responseErr != nil {
			log.Printf(handlers.ErrorEncodingJson, responseErr)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(PullImageResponse{Image: image, Pulled: pulled}); err != nil {
		log.Printf(handlers.ErrorEncodingJson, err)
	}
}

func (a *Api) GetTaskLogsHandler(w http.ResponseWriter, r *http.Request) {
	ref := chi.URLParam(r, "taskID")
	if ref == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	t, found, ambiguous := a.Worker.resolveTask(ref)
	switch {
	case ambiguous:
		msg := fmt.Sprintf("multiple tasks named %q exist; fetch logs by task UUID instead", ref)
		if err := handlers.HttpResponseHelper(w, msg, http.StatusConflict); err != nil {
			log.Printf(handlers.ErrorEncodingJson, err)
		}
		return
	case !found:
		w.WriteHeader(http.StatusNotFound)
		return
	}

	tail := handlers.ParseTail(r)
	result := a.Worker.GetTaskLogs(t, tail)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Task-State", t.State.String())
	if result.ExitCode != nil {
		w.Header().Set("X-Exit-Code", strconv.Itoa(*result.ExitCode))
	}
	w.WriteHeader(http.StatusOK)
	if len(result.Logs) > 0 {
		if _, err := w.Write(result.Logs); err != nil {
			log.Printf("Error writing task logs body to client: %v\n", err)
		}
	}
}
