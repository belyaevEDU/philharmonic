package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

const (
	ErrorUnmarshallingJson      = "Error unmarshalling body: %v\n"
	ErrorEncodingJson           = "Error encoding a response into json: %s\n"
	ErrorEncodingJsonWithTaskID = "Error encoding a response into json for task %s\n: %s"
)

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.TaskEvent{}
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf(ErrorUnmarshallingJson, err)
		err = httpResponseHelper(w, msg, http.StatusBadRequest)
		return
	}

	a.Worker.AddTask(te.Task)
	log.Printf("Added task %v\n", te.Task.ID)
	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(te.Task)
	if err != nil {
		log.Printf(ErrorEncodingJsonWithTaskID, te.Task.ID.String(), err.Error())
	}
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(a.Worker.getTasks())
	if err != nil {
		log.Printf(ErrorEncodingJson, err.Error())
	}
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		msg := "No taskID passed in the request.\n"
		err := httpResponseHelper(w, msg, http.StatusBadRequest)
		if err != nil {
			log.Printf(ErrorEncodingJson, err.Error())
		}
		return
	}

	tID, err := uuid.Parse(taskID)
	if err != nil {
		msg := "Non-UUID taskID passed in the request.\n"
		err = httpResponseHelper(w, msg, http.StatusBadRequest)
		if err != nil {
			log.Printf(ErrorEncodingJson, err.Error())
		}
		return
	}

	taskToStop, exists := a.Worker.Db[tID]
	if !exists {
		msg := fmt.Sprintf("No task with ID %v found", tID)
		err = httpResponseHelper(w, msg, http.StatusNotFound)
		if err != nil {
			log.Printf(ErrorEncodingJsonWithTaskID, taskID, err.Error())
		}
		return
	}

	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)

	msg := fmt.Sprintf(
		"Added task %v to stop container %v\n",
		taskToStop.ID, taskToStop.ContainerID,
	)
	err = httpResponseHelper(w, msg, http.StatusNoContent)
	if err != nil {
		log.Printf(ErrorEncodingJsonWithTaskID, taskID, err.Error())
	}
}

func httpResponseHelper(w http.ResponseWriter, message string, statusCode int) error {
	// moved the log here because for every call of this request
	// im either going to log the message or not
	// regardless of the handler
	log.Print(message)
	w.WriteHeader(statusCode)
	her := HTTPResponse{
		HTTPStatusCode: statusCode,
		Message:        message,
	}

	err := json.NewEncoder(w).Encode(her)
	if err != nil {
		return fmt.Errorf(ErrorEncodingJson, err)
	}

	return nil
}
