package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/belyaevedu/philharmonic/handlers"
	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/task"
)

// the dispatch loop: turns queued task events into worker POSTs

func (m *Manager) SendWork() {
	te, ok := m.dequeuePending()
	if !ok {
		log.Println("No work in the queue")
		return
	}

	t := te.Task
	isStop := te.State == task.Completed || t.State == task.Completed

	var w *node.Node
	reserved := false
	owner := m.taskWorker(t.ID)
	if owner != "" {
		w = m.workerByAddress(owner)
		if w == nil {
			log.Printf("Cannot send task %s: assigned worker %q is unavailable\n", t.ID, owner)
			m.enqueuePending(te)
			return
		}
		if !isStop {
			reserved = m.Reservations.TryReserve(w.Address, &t)
			if !reserved {
				log.Printf("Cannot reserve host ports for task %s on worker %s\n", t.ID, w.Address)
				// port contention on the owning worker is transient:
				// stay retryable without burning a restart slot
				// restartFailedTasks picks this up after backoff and restartTask re-selects then,
				// possibly relocating the task to a worker that can host it
				m.markFailed(t.ID, t.RestartCount,
					"host ports on the assigned worker are reserved by another task")
				return
			}
		}
	} else if isStop {
		// a stop with no owning worker: the record is all there is,
		// so remove it instead of dropping the stop
		if existing, exists := m.getTask(t.ID); exists {
			if err := m.deleteTask(existing); err != nil {
				log.Printf("Error deleting unowned task %s: %v\n", t.ID, err)
				m.enqueuePending(te)
			} else {
				log.Printf("Deleted unowned task %s\n", t.ID)
			}
			return
		}
		log.Printf("Cannot stop task %s: no worker owns it and it no longer exists\n", t.ID)
		return
	} else {
		// a cancellation may have removed the task while it was sitting in the
		// queue; skip dispatch rather than creating a container we'd have to
		// stop immediately. (the post-201 check below is the correctness backstop
		// if the deletion races with this check). nightmare
		if _, exists := m.getTask(t.ID); !exists {
			log.Printf("Task %s was cancelled while queued, skipping dispatch\n", t.ID)
			return
		}
		var err error
		w, err = m.selectAndReserveWorker(&t, "")
		if err != nil {
			if errors.Is(err, errClusterUnreachable) || errors.Is(err, errStatsNotCollected) {
				// total blackout or stats not yet collected
				// infra issue, network blip, manager just booted, etc.
				// keep the task in the pending queue
				log.Printf("Cannot schedule task %s yet (%v); requeueing\n", t.ID, err)
				m.enqueuePending(te)
				return
			}
			// workers are alive but none can host the task
			// terminally stopping the task and letting the user take further action
			reason := fmt.Sprintf("no available worker to schedule task: %v", err)
			log.Printf("Task %s is unschedulable: %v\n", t.ID, err)
			m.stopTaskTerminal(t, reason)
			return
		}
		reserved = true
	}

	if !isStop {
		t.State = task.Scheduled
		te.Task = t
		te.State = task.Scheduled
		if te.Timestamp.IsZero() {
			te.Timestamp = time.Now().UTC()
		}
	}
	if isStop {
		log.Printf("Stopping task %s on worker %s\n", t.ID, w.Address)
	} else if t.RestartCount > 0 {
		log.Printf("Restarting task %s on worker %s\n", t.ID, w.Address)
	} else {
		log.Printf("Starting task %s on worker %s\n", t.ID, w.Address)
	}

	data, err := json.Marshal(te)
	if err != nil {
		if reserved {
			m.Reservations.Release(w.Address, &t)
		}
		log.Printf("Error raised when marshalling task object %v: %v\n", t, err)
		return
	}

	url := httpclient.WorkerURL(w.Address, "/tasks")
	// ignoring gosec's G107 since the url is not from external input, but from an internal config
	resp, err := httpclient.Worker().Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		if reserved {
			m.Reservations.Release(w.Address, &t)
		}
		fmt.Printf("Error connecting to %v: %v\n", w, err)
		m.enqueuePending(te)
		return
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	// the worker refuses stops for tasks it does not know with a 404.
	// so from the cluster's perspective such a stop is already complete
	if isStop && resp.StatusCode == http.StatusNotFound {
		log.Printf("Worker %s has no record of task %s; considering the stop complete\n", w.Address, t.ID)
		m.mu.Lock()
		if m.EventDb == nil {
			log.Printf("Cannot store event %s: event db is nil\n", te.ID)
		} else if err := m.EventDb.Put(te.ID, &te); err != nil {
			log.Printf("Error storing event %s: %v\n", te.ID, err)
		}
		m.mu.Unlock()
		m.Reservations.Release(w.Address, &t)
		return
	}

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		if reserved {
			m.Reservations.Release(w.Address, &t)
		}
		hr := handlers.HTTPResponse{}
		if err := d.Decode(&hr); err != nil {
			fmt.Printf("Error decoding response: %v\n", err)
		} else {
			log.Printf("Response error (%d): %s", hr.HTTPStatusCode, hr.Message)
		}
		if isStop {
			// a rejected stop leaves the task running.
			// we neither requeue (would loop forever against the fixed owning worker)
			// nor mark Failed (would make restartFailedTasks try to *restart* a task
			// the user asked to stop). just log it and hope for the best
			return
		}
		// rejection of a start/restart: marking it failed w/o term to try & restart again
		reason := fmt.Sprintf("worker %s rejected task (%d)", w.Address, resp.StatusCode)
		if hr.Message != "" {
			reason = fmt.Sprintf("%s: %s", reason, hr.Message)
		}
		m.markFailed(t.ID, t.RestartCount+1, reason)
		return
	}

	respTask := task.Task{}
	decodeErr := d.Decode(&respTask)
	if decodeErr != nil {
		fmt.Printf("Error decoding response: %v\n", decodeErr)
	}

	// the worker echoes the accepted event before its run loop has assigned a fresh container ID.
	// fall back to the event if the response is malformed. the worker can resolve the stop by task ID from its own record
	cleanupTask := respTask
	if cleanupTask.ID != t.ID {
		cleanupTask = te.Task
	}

	var (
		cancelled            bool
		commitErr            error
		shouldRetryCommit    bool
		oldAssignment        Assignment
		hadOldAssignment     bool
		originalTask         task.Task
		assignmentRestoreErr error
		taskRestoreErr       error
	)

	m.mu.Lock()
	if m.EventDb == nil {
		log.Printf("Cannot store event %s: event db is nil\n", te.ID)
	} else if err := m.EventDb.Put(te.ID, &te); err != nil {
		log.Printf("Error storing event %s: %v\n", te.ID, err)
	}
	if !isStop {
		// a cancellation may have deleted the task while the POST was in flight.
		// if so, don't resurrect it. release the ports we reserved and issue a
		// compensating stop for the task the worker just accepted
		if m.TaskDb == nil {
			commitErr = errors.New("task db is nil")
		} else if persisted, getErr := m.TaskDb.Get(t.ID); getErr != nil {
			if errors.Is(getErr, store.ErrNotFound) {
				cancelled = true
			} else {
				commitErr = fmt.Errorf("checking task %s before commit: %w", t.ID, getErr)
				shouldRetryCommit = true
			}
		} else if persisted == nil {
			commitErr = fmt.Errorf("checking task %s before commit: store returned a nil record", t.ID)
			shouldRetryCommit = true
		} else {
			originalTask = *persisted
			oldAssignment, hadOldAssignment, commitErr = readAssignmentState(m.Assignments, t.ID)
			if commitErr != nil {
				shouldRetryCommit = true
			} else {
				// Both manager writes happen after the worker's 201. Any failure
				// is compensated below before this event is retried.
				if err := m.setAssignment(t.ID, w.Address); err != nil {
					commitErr = fmt.Errorf("storing assignment for task %s: %w", t.ID, err)
					assignmentRestoreErr = restoreAssignment(m.Assignments, t.ID, oldAssignment, hadOldAssignment)
				} else if err := m.TaskDb.Put(t.ID, &t); err != nil {
					commitErr = fmt.Errorf("storing task %s: %w", t.ID, err)
					assignmentRestoreErr = restoreAssignment(m.Assignments, t.ID, oldAssignment, hadOldAssignment)
					taskRestoreErr = m.TaskDb.Put(t.ID, &originalTask)
				}
				if commitErr != nil {
					shouldRetryCommit = true
				}
			}
		}
	}
	m.mu.Unlock()

	if cancelled {
		m.rollbackAcceptedWorkerTask(w.Address, t, cleanupTask, reserved)
		log.Printf("Task %s was cancelled while starting on worker %s; stopping orphaned task\n", t.ID, w.Address)
		return
	}
	if commitErr != nil {
		if assignmentRestoreErr != nil {
			log.Printf("Could not restore assignment for task %s after dispatch commit failure: %v\n", t.ID, assignmentRestoreErr)
		}
		if taskRestoreErr != nil {
			log.Printf("Could not restore task %s after dispatch commit failure: %v\n", t.ID, taskRestoreErr)
		}
		m.rollbackAcceptedWorkerTask(w.Address, t, cleanupTask, reserved)
		log.Printf("Could not commit task %s after worker %s accepted it: %v\n", t.ID, w.Address, commitErr)
		if shouldRetryCommit {
			m.enqueuePending(te)
		}
		return
	}

	m.updateWaker.Wake()

	if isStop {
		m.Reservations.Release(w.Address, &t)
	}

	if decodeErr == nil {
		log.Printf("%#v\n", respTask) // # adds field names
	}
}
