package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/google/uuid"
)

// reconciliation with workers: task state sync and orphan cleanup

func (m *Manager) fetchTasksFromWorker(worker string) ([]*task.Task, error) {
	url := httpclient.WorkerURL(worker, "/tasks")
	// ignoring gosec's G107 since the url is not from external input, but from an internal config
	resp, err := httpclient.Worker().Get(url) // #nosec G107
	if err != nil {
		return nil, fmt.Errorf("error connecting to worker: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("worker responded with status %d", resp.StatusCode)
	}

	var tasks []*task.Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, fmt.Errorf("error unmarshalling tasks: %w", err)
	}
	return tasks, nil
}

type WorkerLogsResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// proxies a logs request to the owning worker
func (m *Manager) fetchTaskLogsFromWorker(worker string, taskID uuid.UUID, tail int) (*WorkerLogsResponse, error) {
	path := "/tasks/logs/" + taskID.String()
	if tail > 0 {
		path += "?tail=" + strconv.Itoa(tail)
	}
	url := httpclient.WorkerURL(worker, path)
	// ignoring gosec's G107/G704: no request-controlled input reaches this URL
	resp, err := httpclient.Worker().Get(url) // #nosec G107 G704
	if err != nil {
		return nil, fmt.Errorf("error connecting to worker: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing logs response body from worker %s: %v\n", worker, err)
		}
	}()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProxyLogSize))
	if err != nil {
		return nil, fmt.Errorf("error reading logs response body from worker %s: %w", worker, err)
	}
	return &WorkerLogsResponse{StatusCode: resp.StatusCode, Header: resp.Header, Body: body}, nil
}

func (m *Manager) updateTasks() {
	for _, n := range m.WorkerNodes {
		tasks, err := m.fetchTasksFromWorker(n.Address)
		if err != nil {
			log.Printf("Error fetching tasks from %s: %v\n", n.Address, err)
			continue
		}

		for _, t := range tasks {
			m.mu.Lock()
			if m.TaskDb == nil {
				m.mu.Unlock()
				log.Printf("Task db is nil; cannot update task %s\n", t.ID.String())
				continue
			}

			persisted, err := m.TaskDb.Get(t.ID)
			if err != nil {
				m.mu.Unlock()
				if errors.Is(err, store.ErrNotFound) {
					// the manager no longer knows this task (it was deleted), so
					// the worker's record is an orphan. have the worker drop it
					m.forgetTaskOnWorker(n.Address, t.ID)
				} else {
					log.Printf("Error getting task %s: %v\n", t.ID.String(), err)
				}
				continue
			}

			// a worker can still be returning a snapshot from the previous restart attempt
			// while the manager has already queued the next one
			if t.RestartCount != persisted.RestartCount {
				m.mu.Unlock()
				continue
			}

			updated := *persisted
			updated.ContainerID = t.ContainerID
			updated.HostPorts = t.HostPorts
			if !t.StartTime.IsZero() {
				updated.StartTime = t.StartTime
			}
			if !t.FinishTime.IsZero() {
				updated.FinishTime = t.FinishTime
			}

			if t.State == task.Failed {
				if persisted.State != task.Failed {
					// first observation
					updated.NextRetryAt = time.Now().UTC().Add(restartBackoff(updated.RestartCount))
				}
				updated.State = task.Failed
				if t.FailureReason != "" {
					updated.FailureReason = t.FailureReason
				}
			} else if !updated.IsTerminal() {
				// not terminal-failed -> trust the worker's state
				updated.State = t.State
			}
			// else: terminal-failed here

			changed := !reflect.DeepEqual(*persisted, updated)
			if err := m.TaskDb.Put(t.ID, &updated); err != nil {
				log.Printf("Error updating task %s: %v\n", t.ID, err)
			} else if changed && persisted.State != updated.State {
				log.Printf("Task %s changed state from %s to %s\n", t.ID, persisted.State, updated.State)
			} else if changed {
				log.Printf("Task %s changed\n", t.ID)
			}
			m.mu.Unlock()
		}
	}
}

// asks a worker to drop its record of a task the manager no longer knows.
// the worker stops a still-active container before forgetting,
// so the DELETE can outlast a short client timeout.
// the manager simply retries on the next tick, where a finished forget answers 404
func (m *Manager) forgetTaskOnWorker(addr string, id uuid.UUID) {
	url := httpclient.WorkerURL(addr, "/tasks/"+id.String())
	req, err := http.NewRequest(http.MethodDelete, url, nil) // #nosec G107
	if err != nil {
		log.Printf("Error building forget request for task %s on worker %s: %v\n", id, addr, err)
		return
	}
	resp, err := httpclient.Worker().Do(req)
	if err != nil {
		log.Printf("Could not ask worker %s to forget task %s: %v\n", addr, id, err)
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing forget response body from worker %s: %v\n", addr, err)
		}
	}()
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusNotFound:
		// forgotten, or already forgotten
	default:
		log.Printf("Worker %s could not forget task %s: status %d\n", addr, id, resp.StatusCode)
	}
}
