package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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

	a.Worker.AddTask(te.Task)
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
	a.Worker.AddTask(taskCopy)

	log.Printf("Added task %v to stop container %v\n", taskToStop.ID, taskToStop.ContainerID)
	w.WriteHeader(http.StatusNoContent)
}

func (a *Api) GetStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(a.Worker.Stats)
	if err != nil {
		log.Printf(handlers.ErrorEncodingJson, err)
	}
}
