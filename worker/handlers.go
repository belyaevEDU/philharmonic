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
	// will possibly transition to returning the errors

	// these errors are still being straight up logged,
	// so for the time being they are starting with an uppercase character
	// and ending with a newline character
	ErrorUnmarshallingJson      = "Error unmarshalling body: %v\n"
	ErrorEncodingJson           = "Error encoding a response into json: %v\n"
	ErrorEncodingJsonWithTaskID = "Error encoding a response into json for task %s: %v\n"
)

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.TaskEvent{}
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf(ErrorUnmarshallingJson, err)
		err = httpResponseHelper(w, msg, http.StatusBadRequest)
		if err != nil {
			log.Printf(ErrorEncodingJson, err.Error())
		}
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

	taskToStop, exists := a.Worker.Db[tID]
	if !exists {
		log.Printf("No task with ID %v found\n", tID)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)

	log.Printf("Added task %v to stop container %v\n", taskToStop.ID, taskToStop.ContainerID)
	w.WriteHeader(http.StatusNoContent)
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
		// not using the consts due to the fact that for the time being
		// they start with an uppercase letter and end with a newline character
		return fmt.Errorf("error raised when encoding a response: %w", err)
	}

	return nil
}
