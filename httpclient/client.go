package httpclient

import (
	"net/http"
	"time"
)

var ClientTimeout = 10 * time.Second

func New() *http.Client {
	return &http.Client{Timeout: ClientTimeout}
}
