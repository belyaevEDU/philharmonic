package cmd

import (
	"net/http"
	"time"
)

var HTTPClientTimeout = 10 * time.Second

func newHTTPClient() *http.Client {
	return &http.Client{Timeout: HTTPClientTimeout}
}
