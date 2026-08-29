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
	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/google/uuid"
)

// failure and restart machinery: backoff, restarts, and terminal stops

// exponential restart backoff bounds
// restart attempt n = base * 2^(n-1), capped at max
var (
	RestartBackoffBase = 10 * time.Second
	RestartBackoffMax  = 5 * time.Minute
)

// base * 2^(restartCount-1), capped at RestartBackoffMax
func restartBackoff(restartCount int) time.Duration {
	d := RestartBackoffBase
	for i := 1; i < restartCount && d < RestartBackoffMax; i++ {
		d *= 2
		// clamp to the cap if overflow hits
		if d < 0 {
			return RestartBackoffMax
		}
	}
	if d > RestartBackoffMax {
		return RestartBackoffMax
	}
	return d
}

func (m *Manager) restartTask(t task.Task) error {
	// restartTask schedules a restart of t on its owning worker
	// for a task with no owner re-schedules it via the scheduler
	owner := m.taskWorker(t.ID)

	next := t
	next.State = task.Scheduled
	next.RestartCount = t.RestartCount + 1
	next.FailureReason = ""
	next.StartTime = time.Time{}
	next.FinishTime = time.Time{}

	w, err := m.selectAndReserveWorker(&next, owner)
	if err != nil {
		if errors.Is(err, errClusterUnreachable) || errors.Is(err, errStatsNotCollected) {
			// infra issue / stats not yet collected, transient
			reason := fmt.Sprintf("cannot restart yet: %v", err)
			m.markFailed(t.ID, t.RestartCount, reason)
			return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
		}
		// workers are alive but none can host the task
		// terminally stopping the task and letting the user take further action
		reason := fmt.Sprintf("no available worker to restart: %v", err)
		m.stopTaskTerminal(t, reason)
		return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
	}

	if owner == "" || w.Address != owner {
		next.ContainerID = ""
		next.HostPorts = nil
	}

	log.Printf("Restarting task %s on worker %s\n", t.ID, w.Address)

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Scheduled,
		Timestamp: time.Now().UTC(),
		Task:      next,
	}

	data, err := json.Marshal(te)
	if err != nil {
		m.Reservations.Release(w.Address, &next)
		reason := fmt.Sprintf("unable to marshal restart event: %v", err)
		m.markFailed(t.ID, t.RestartCount+1, reason)
		return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
	}

	url := httpclient.WorkerURL(w.Address, "/tasks")
	// ignoring gosec's G107 since the url is not from external input, but from an internal config
	resp, err := httpclient.Worker().Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		m.Reservations.Release(w.Address, &next)
		// the worker never saw the restart, so don't burn a restart slot
		reason := fmt.Sprintf("could not reach worker %s to restart: %v", w.Address, err)
		m.markFailed(t.ID, t.RestartCount, reason)
		return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		m.Reservations.Release(w.Address, &next)
		hr := handlers.HTTPResponse{}
		if err := d.Decode(&hr); err != nil {
			log.Printf("Error decoding rejection response: %v\n", err)
		}
		// the worker refused the restart, so burn a slot and
		// let restartFailedTasks bound the retries via MaxRestarts
		reason := fmt.Sprintf("worker %s rejected restart (%d)", w.Address, resp.StatusCode)
		if hr.Message != "" {
			reason = fmt.Sprintf("%s: %s", reason, hr.Message)
		}
		m.markFailed(t.ID, t.RestartCount+1, reason)
		return fmt.Errorf("cannot restart task %s: %s", t.ID, reason)
	}

	respTask := task.Task{}
	if err := d.Decode(&respTask); err != nil {
		log.Printf("Error decoding restart response: %v\n", err)
	}
	// the worker normally echoes the accepted event before its run loop has assigned a fresh container ID
	cleanupTask := respTask
	if cleanupTask.ID != next.ID {
		cleanupTask = next
	}

	m.mu.Lock()
	if m.TaskDb == nil {
		m.mu.Unlock()
		m.rollbackAcceptedWorkerTask(w.Address, next, cleanupTask, true)
		return fmt.Errorf("cannot store restarted task %s: task db is nil", t.ID)
	}

	persisted, getErr := m.TaskDb.Get(t.ID)

	switch {
	case getErr != nil && errors.Is(getErr, store.ErrNotFound):
		m.mu.Unlock()
		// the task was deleted while the restart was in flight
		m.rollbackAcceptedWorkerTask(w.Address, next, cleanupTask, true)
		return fmt.Errorf("task %s was deleted while being restarted", t.ID)
	case getErr != nil:
		m.mu.Unlock()
		// an unreadable manager record is unsafe to overwrite
		// undo the worker-side restart and let the next retry try again
		m.rollbackAcceptedWorkerTask(w.Address, next, cleanupTask, true)
		return fmt.Errorf("cannot read restarted task %s before commit: %w", t.ID, getErr)
	case persisted.ManuallyStopped:
		m.mu.Unlock()
		// the user asked to stop the task while the restart was in flight
		m.rollbackAcceptedWorkerTask(w.Address, next, cleanupTask, true)
		return fmt.Errorf("task %s was stopped while being restarted", t.ID)
	}

	originalTask := *persisted
	oldAssignment, hadOldAssignment, assignmentErr := readAssignmentState(m.Assignments, t.ID)
	if assignmentErr != nil {
		m.mu.Unlock()
		m.rollbackAcceptedWorkerTask(w.Address, next, cleanupTask, true)
		return fmt.Errorf("cannot read assignment for restarted task %s: %w", t.ID, assignmentErr)
	}

	// Both manager writes happen after the worker's 201. Any failure must be
	// compensated before retrying, or the worker would retain a live task while
	// the manager retained only the old record.
	if err := m.setAssignment(t.ID, w.Address); err != nil {
		restoreErr := restoreAssignment(m.Assignments, t.ID, oldAssignment, hadOldAssignment)
		m.mu.Unlock()
		if restoreErr != nil {
			log.Printf("Could not restore assignment for task %s after commit failure: %v\n", t.ID, restoreErr)
		}
		m.rollbackAcceptedWorkerTask(w.Address, next, cleanupTask, true)
		return fmt.Errorf("cannot store assignment for restarted task %s: %w", t.ID, err)
	}
	if err := m.TaskDb.Put(next.ID, &next); err != nil {
		assignmentRestoreErr := restoreAssignment(m.Assignments, t.ID, oldAssignment, hadOldAssignment)
		taskRestoreErr := m.TaskDb.Put(t.ID, &originalTask)
		m.mu.Unlock()
		if assignmentRestoreErr != nil {
			log.Printf("Could not restore assignment for task %s after task commit failure: %v\n", t.ID, assignmentRestoreErr)
		}
		if taskRestoreErr != nil {
			log.Printf("Could not restore task %s after task commit failure: %v\n", t.ID, taskRestoreErr)
		}
		m.rollbackAcceptedWorkerTask(w.Address, next, cleanupTask, true)
		return fmt.Errorf("cannot store restarted task %s: %w", t.ID, err)
	}
	m.mu.Unlock()

	oldOwner := ""
	if hadOldAssignment {
		oldOwner = oldAssignment.Worker
	}
	if oldOwner != "" && oldOwner != w.Address {
		m.Reservations.Release(oldOwner, &t)
		m.bestEffortStopOldContainer(oldOwner, t)
	}

	log.Printf("Restarted task %s on worker %s\n", t.ID, w.Address)
	m.updateWaker.Wake()
	return nil
}

// records a task as Failed with the given restart count and reason,
// clearing any prior terminal stamp (FinishTime = 0)
// and arming the restart backoff window,
// so restartFailedTasks can start the task again after a pause
func (m *Manager) markFailed(id uuid.UUID, restartCount int, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.TaskDb == nil {
		log.Printf("Cannot mark task %s as failed: task db is nil\n", id)
		return
	}

	persisted, err := m.TaskDb.Get(id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// the task was deleted concurrently
			log.Printf("Not marking unknown task %s as failed\n", id)
		} else {
			log.Printf("Cannot mark task %s as failed: %v\n", id, err)
		}
		return
	}

	updated := *persisted
	updated.State = task.Failed
	updated.RestartCount = restartCount
	updated.FailureReason = reason
	updated.FinishTime = time.Time{}
	updated.NextRetryAt = time.Now().UTC().Add(restartBackoff(restartCount))
	if err := m.TaskDb.Put(id, &updated); err != nil {
		log.Printf("Cannot mark task %s as failed: %v\n", id, err)
	}
}

// records that the user stopped the task,
// so restart policies must not resurrect it later
func (m *Manager) markStopRequested(id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.TaskDb == nil {
		return errors.New("task db is nil")
	}

	persisted, err := m.TaskDb.Get(id)
	if err != nil {
		return fmt.Errorf("getting task %s: %w", id, err)
	}
	updated := *persisted
	updated.ManuallyStopped = true
	if err := m.TaskDb.Put(id, &updated); err != nil {
		return fmt.Errorf("storing task %s: %w", id, err)
	}
	return nil
}

// sends a best-effort stop event to a worker for a task.
// the container ID may be empty because the worker accepts a start event before its run loop creates the container.
// the worker then resolves the stop against its own current task record.
//
// errors are just logged
func (m *Manager) bestEffortStopWorkerTask(addr string, t task.Task) {
	if addr == "" || t.ID == uuid.Nil {
		return
	}
	stopTask := t
	stopTask.State = task.Completed
	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now().UTC(),
		Task:      stopTask,
	}
	data, err := json.Marshal(te)
	if err != nil {
		log.Printf("Error marshalling cleanup stop for task %s: %v", t.ID, err)
		return
	}
	url := httpclient.WorkerURL(addr, "/tasks")
	resp, err := httpclient.Worker().Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		log.Printf("Could not reach worker %s to stop task %s: %v", addr, t.ID, err)
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v", err)
		}
	}()
	if resp.StatusCode != http.StatusCreated {
		log.Printf("Worker %s responded %d stopping task %s", addr, resp.StatusCode, t.ID)
	}
}

// used when a restart relocates a task to a different worker so the previous container isn't orphaned.
// unlike the generic cleanup above, this helper requires a known old container ID.
func (m *Manager) bestEffortStopOldContainer(addr string, t task.Task) {
	if t.ContainerID == "" {
		return
	}
	m.bestEffortStopWorkerTask(addr, t)
}

// undoes the worker-side part of a restart/start whose manager-side commit
// failed after the worker accepted it
func (m *Manager) rollbackAcceptedWorkerTask(addr string, reservationTask, stopTask task.Task, reserved bool) {
	if reserved {
		m.Reservations.Release(addr, &reservationTask)
	}
	m.bestEffortStopWorkerTask(addr, stopTask)
}

func (m *Manager) stopTaskTerminal(t task.Task, reason string) {
	m.mu.Lock()
	w := readAssignment(m.Assignments, t.ID)
	t.State = task.Failed
	t.FailureReason = reason
	t.FinishTime = time.Now().UTC()
	if m.TaskDb == nil {
		log.Printf("Cannot store terminal task %s: task db is nil\n", t.ID)
	} else if err := m.TaskDb.Put(t.ID, &t); err != nil {
		log.Printf("Error storing terminal task %s: %v\n", t.ID, err)
	}
	m.mu.Unlock()

	m.Reservations.Release(w, &t)
	if w == "" {
		log.Printf("No worker owns terminal task %s; cannot send cleanup stop\n", t.ID)
		return
	}

	stopTask := t
	stopTask.State = task.Completed // will be Failed in the end since we sent over the failure reason
	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
		Task:      stopTask,
	}

	data, err := json.Marshal(te)
	if err != nil {
		log.Printf("Error marshalling task object: %v\n", t)
		return
	}

	url := httpclient.WorkerURL(w, "/tasks")
	resp, err := httpclient.Worker().Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		log.Printf("Error connecting to %v: %v", w, err)
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	if resp.StatusCode != http.StatusCreated {
		log.Printf("Error: worker %s responded with %d when stopping terminal task %s\n", w, resp.StatusCode, t.ID)
	}
}

// http/tcp health checks

func (m *Manager) restartFailedTasks() {
	now := time.Now().UTC()
	for _, t := range m.getTasks() {
		switch t.State {
		case task.Failed:
			// the user asked to stop this task; no policy may override that
			if t.ManuallyStopped {
				continue
			}

			// a terminal stamp means stopTaskTerminal already ended this task.
			// retryable failures always carry FinishTime = 0 (markFailed clears it)
			if t.IsTerminal() {
				continue
			}

			if !task.ShouldRestart(t.RestartPolicy) {
				reason := fmt.Sprintf("restart policy %q does not permit a restart", t.RestartPolicy)
				log.Printf("Task %s failed and its restart policy forbids a restart; stopping its container\n", t.ID)
				m.stopTaskTerminal(t, reason)
				continue
			}

			restartCap := t.EffectiveMaxRestarts(MaxRestarts)
			if t.AtRestartCap(MaxRestarts) {
				reason := t.FailureReason
				if reason == "" {
					reason = fmt.Sprintf("restart cap (%d) reached", restartCap)
				}
				log.Printf("Task %s reached its restart cap (%d); marking failed and stopping its container\n", t.ID, restartCap)
				m.stopTaskTerminal(t, reason)
				continue
			}

			// within the backoff window armed by the last failure. wait it out
			if now.Before(t.NextRetryAt) {
				continue
			}

			if err := m.restartTask(t); err != nil {
				log.Printf("Error restarting task %s: %v", t.ID, err)
			}

		case task.Completed:
			// a clean exit is only restartable under always/unless-stopped,
			// and never after a manual stop
			if t.ManuallyStopped || !task.ShouldRestartOnSuccess(t.RestartPolicy) {
				continue
			}

			// at the restart cap a cleanly exited task stays Completed:
			// there is no failure to record and no container left to stop.
			// the cap is derivable from the record (RestartCount vs MaxRestarts),
			// so this is a silent skip rather than a log every loop iteration
			if t.AtRestartCap(MaxRestarts) {
				continue
			}

			if now.Before(t.NextRetryAt) {
				continue
			}

			log.Printf("Restarting cleanly exited task %s (restart policy %q)\n", t.ID, t.RestartPolicy)
			if err := m.restartTask(t); err != nil {
				log.Printf("Error restarting task %s: %v", t.ID, err)
			}
		}
	}
}
