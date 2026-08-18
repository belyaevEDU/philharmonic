package utils

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

func HTTPWithRetry(f func(string) (*http.Response, error), url string, maxTries int, period time.Duration) (*http.Response, error) {
	if maxTries <= 0 || period <= 0 {
		return nil, errors.New("invalid argument, needs to be positive")
	}

	var resp *http.Response
	var err error
	for i := 0; i < maxTries; i++ {
		resp, err = f(url)
		if err != nil {
			fmt.Printf("Error calling url %v: %v\n", url, err)
			time.Sleep(period)
			if i != (maxTries - 1) {
				err = nil
			}
		} else {
			break
		}
	}

	return resp, err
}
