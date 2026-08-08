package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/belyaevedu/philharmonic/task"
)

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.TaskEvent{}
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		httpErrResponseHelper(w, msg, http.StatusBadRequest)
		return
	}

	a.Worker.AddTask(te.Task)
	log.Printf("Added task %v\n", te.Task.ID)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(te.Task)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Worker.getTasks())
}

func httpErrResponseHelper(w http.ResponseWriter, message string, statusCode int) {
	// moved the log here because i think for every call of this request
	// im either going to log the message or not
	// regardless of the handler
	log.Print(message)
	w.WriteHeader(statusCode)
	her := HTTPErrResponse{
		HTTPStatusCode: statusCode,
		Message:        message,
	}
	json.NewEncoder(w).Encode(her)
}
