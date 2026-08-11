package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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

type HTTPResponse struct {
	HTTPStatusCode int
	Message        string
}

func HttpResponseHelper(w http.ResponseWriter, message string, statusCode int) error {
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
