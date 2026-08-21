package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/belyaevedu/philharmonic/handlers"
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

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
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

	taskToStop, exists := a.Worker.getTask(tID)
	if !exists {
		log.Printf("No task with ID %v found\n", tID)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	taskCopy := taskToStop
	taskCopy.State = task.Completed
	if err := a.Worker.AddTask(taskCopy); err != nil {
		if responseErr := handlers.HttpResponseHelper(w, err.Error(), http.StatusInternalServerError); responseErr != nil {
			log.Printf(handlers.ErrorEncodingJson, responseErr)
		}
		return
	}

	log.Printf("Added task %v to stop container %v\n", taskToStop.ID, taskToStop.ContainerID)
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

	tail := parseTail(r)
	result := a.Worker.GetTaskLogs(t, tail)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Task-State", t.State.String())
	if result.ExitCode != nil {
		w.Header().Set("X-Exit-Code", strconv.Itoa(*result.ExitCode))
	}
	w.WriteHeader(http.StatusOK)
	if len(result.Logs) > 0 {
		_, _ = w.Write(result.Logs)
	}
}

// extracts the ?tail= query param as a line count
// <= 0 or absent means "all lines"
func parseTail(r *http.Request) int {
	raw := r.URL.Query().Get("tail")
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
