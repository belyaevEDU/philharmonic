package manager

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/belyaevedu/philharmonic/auth"
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

	te.ID = uuid.New()

	if te.State != task.Completed && te.Task.State != task.Completed {
		if err := task.ValidatePortMappings(te.Task.Ports); err != nil {
			if responseErr := handlers.HttpResponseHelper(w, err.Error(), http.StatusBadRequest); responseErr != nil {
				log.Printf(handlers.ErrorEncodingJson, responseErr)
			}
			return
		}
		if te.Task.ID == uuid.Nil {
			te.Task.ID = uuid.New()
		}
	}

	err = a.Manager.AddTask(te)
	if err != nil {
		msg := fmt.Sprintf("Error adding task: %v\n", err)
		responseErr := handlers.HttpResponseHelper(w, msg, http.StatusConflict)
		if responseErr != nil {
			log.Printf(handlers.ErrorEncodingJson, responseErr)
		}
		return
	}

	if id, ok := auth.IdentityFromContext(r.Context()); ok {
		log.Printf("Added task %v (user %q)\n", te.Task.ID, id.User)
	} else {
		log.Printf("Added task %v\n", te.Task.ID)
	}
	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(te.Task)
	if err != nil {
		log.Printf(handlers.ErrorEncodingJsonWithTaskID, te.Task.ID.String(), err.Error())
	}
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(a.Manager.getTaskViews())
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

	var taskToStop task.Task
	var found bool

	if tID, err := uuid.Parse(taskID); err == nil {
		taskToStop, found = a.Manager.getTask(tID)
		if !found {
			log.Printf("No task with ID %v found\n", tID)
		}
	} else {
		var ambiguous bool
		taskToStop, found, ambiguous = a.Manager.getTaskByName(taskID)
		switch {
		case ambiguous:
			msg := fmt.Sprintf("multiple tasks named %q exist; stop by task UUID instead", taskID)
			responseErr := handlers.HttpResponseHelper(w, msg, http.StatusConflict)
			if responseErr != nil {
				log.Printf(handlers.ErrorEncodingJson, responseErr)
			}
			return
		case !found:
			log.Printf("No task with ID or name %q found\n", taskID)
		}
	}

	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// stopping a Pending task cancels its queued start and removes it from the
	// db; it has not reached a worker yet, so there is no owner to stop.
	// Completed tasks are also cleaned up. A Failed task is cleaned up only
	// after it has reached the restart cap
	if taskToStop.State == task.Pending || taskToStop.State == task.Completed ||
		(taskToStop.State == task.Failed && taskToStop.RestartCount >= MaxRestarts) {
		if err := a.Manager.deleteTask(taskToStop); err != nil {
			msg := fmt.Sprintf("Error deleting task %s: %v\n", taskToStop.ID, err)
			responseErr := handlers.HttpResponseHelper(w, msg, http.StatusInternalServerError)
			if responseErr != nil {
				log.Printf(handlers.ErrorEncodingJson, responseErr)
			}
			return
		}
		log.Printf("Deleted task %v (state %s, name %q)\n", taskToStop.ID, taskToStop.State, taskToStop.Name)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	taskCopy := taskToStop
	taskCopy.State = task.Completed

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
		Task:      taskCopy,
	}

	if err := a.Manager.AddTask(te); err != nil {
		msg := fmt.Sprintf("Error queuing stop for task %s: %v\n", taskToStop.ID, err)
		responseErr := handlers.HttpResponseHelper(w, msg, http.StatusInternalServerError)
		if responseErr != nil {
			log.Printf(handlers.ErrorEncodingJson, responseErr)
		}
		return
	}

	log.Printf("Added task %v to stop container %v\n", taskToStop.ID, taskToStop.ContainerID)
	w.WriteHeader(http.StatusNoContent)
}

func (a *Api) GetNodesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(a.Manager.getNodeViews())
	if err != nil {
		log.Printf(handlers.ErrorEncodingJson, err.Error())
	}
}

// proxies a logs request to the worker that owns the task
//
// {taskID} may be a UUID or a task name
//
// the response is text/plain with X-Task-State and X-Exit-Code headers
func (a *Api) GetTaskLogsHandler(w http.ResponseWriter, r *http.Request) {
	ref := chi.URLParam(r, "taskID")
	if ref == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	t, found, ambiguous := a.Manager.resolveTask(ref)
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

	worker := a.Manager.taskWorker(t.ID)
	if worker == "" {
		// task never reached a worker
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Task-State", t.State.String())
		w.WriteHeader(http.StatusOK)
		return
	}

	tail := r.URL.Query().Get("tail")
	resp, err := a.Manager.fetchTaskLogsFromWorker(worker, t.ID, tail)
	if err != nil {
		msg := fmt.Sprintf("Error fetching logs from worker %s: %v\n", worker, err)
		if responseErr := handlers.HttpResponseHelper(w, msg, http.StatusBadGateway); responseErr != nil {
			log.Printf(handlers.ErrorEncodingJson, responseErr)
		}
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing logs response body: %v\n", err)
		}
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		// relay the worker's text body and metadata headers
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Task-State", resp.Header.Get("X-Task-State"))
		if ec := resp.Header.Get("X-Exit-Code"); ec != "" {
			w.Header().Set("X-Exit-Code", ec)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, resp.Body)
	case http.StatusNotFound:
		// container gone and no stored logs, so empty 200 with just the state
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Task-State", t.State.String())
		w.WriteHeader(http.StatusOK)
	default:
		// worker returned an error status, surface it as 502
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			msg := fmt.Sprintf("worker %s returned status %d fetching logs, couldn't read resp body: %s",
				worker, resp.StatusCode, err)
			if responseErr := handlers.HttpResponseHelper(w, msg, http.StatusBadGateway); responseErr != nil {
				log.Printf(handlers.ErrorEncodingJson, responseErr)
			}
		}
		msg := fmt.Sprintf("worker %s returned status %d fetching logs: %s",
			worker, resp.StatusCode, handlers.HTTPResponse{}.Message)
		if len(body) > 0 {
			msg = fmt.Sprintf("worker %s returned status %d fetching logs: %s",
				worker, resp.StatusCode, string(body))
		}
		if responseErr := handlers.HttpResponseHelper(w, msg, http.StatusBadGateway); responseErr != nil {
			log.Printf(handlers.ErrorEncodingJson, responseErr)
		}
	}
}
