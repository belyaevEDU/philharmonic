package manager

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/google/uuid"
)

// manager-driven http/tcp health checks

// http/tcp health checks
func (m *Manager) checkTaskHealth(ctx context.Context, t task.Task, w string) error {
	hc := t.HealthCheck

	host, _, err := net.SplitHostPort(w)
	if err != nil {
		return fmt.Errorf("invalid worker address %q: %w", w, err)
	}
	hostPort := hostPortFor(t.HostPorts, hc.Port)
	if hostPort == 0 {
		return fmt.Errorf("error: task %s has no published host port for container port %d", t.ID, hc.Port)
	}

	switch hc.Type {
	case task.HealthCheckHTTP:
		path := hc.Path
		if path == "" {
			path = "/"
		}
		url := fmt.Sprintf("http://%s%s", net.JoinHostPort(host, strconv.Itoa(hostPort)), path)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("error building request: %w", err)
		}
		resp, err := httpclient.Plain().Do(req)
		if err != nil {
			return fmt.Errorf("error performing health check %s: %w", url, err)
		}
		defer func() {
			err = resp.Body.Close()
			if err != nil {
				log.Printf("Error closing response body: %v\n", err)
			}
		}()

		body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		if err != nil {
			return fmt.Errorf("error reading health check response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			output := strings.TrimSpace(string(body))
			if output != "" {
				return fmt.Errorf("health check %s returned %d: %s", url, resp.StatusCode, output)
			}

			return fmt.Errorf("health check %s returned %d", url, resp.StatusCode)
		}
		return nil

	case task.HealthCheckTCP:
		addr := net.JoinHostPort(host, strconv.Itoa(hostPort))

		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("tcp health check %s: %w", addr, err)
		}

		err = conn.Close()
		if err != nil {
			return fmt.Errorf("error closing tcp health check: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("unsupported manager-driven health check type %s", hc.Type)
	}
}

func (m *Manager) reconcileCheckers(ctx context.Context) {
	seen := make(map[uuid.UUID]struct{})

	for _, t := range m.getTasks() {
		seen[t.ID] = struct{}{}

		// only http/tcp health checks are managed by the manager
		managed := t.State == task.Running && t.HealthCheck != nil &&
			(t.HealthCheck.Type == task.HealthCheckHTTP || t.HealthCheck.Type == task.HealthCheckTCP)
		if !managed {
			m.stopChecker(t.ID)
			continue
		}

		if !m.checkers.has(t.ID) {
			m.startChecker(ctx, t)
			log.Printf("Started %s health checker for task %s\n", t.HealthCheck.Type, t.ID)
		}
	}

	m.checkers.stopAllExcept(seen)
}

func (m *Manager) startChecker(ctx context.Context, t task.Task) {
	// derive each per-task checker ctx from the manager's root ctx so a shutdown
	// (root cancel) tears down all running checkers, not just the ones
	// reconcileCheckers stops
	ctx, cancel := context.WithCancel(ctx)
	h := &checkerHandle{cancel: cancel}
	if !m.checkers.start(t.ID, h) {
		// a checker is already running; drop the fresh ctx to avoid a leak
		cancel()
		return
	}
	go m.runChecker(ctx, t, h)
}

func (m *Manager) stopChecker(id uuid.UUID) {
	m.checkers.stop(id)
}

func (m *Manager) runChecker(ctx context.Context, t task.Task, h *checkerHandle) {
	defer m.checkers.finished(t.ID, h)

	hc := t.HealthCheck.Normalized()

	w := m.taskWorker(t.ID)
	if w == "" {
		log.Printf("Task %s is no longer assigned to a worker, stopping its checker\n", t.ID)
		return
	}

	if hc.StartPeriod > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(hc.StartPeriod) * time.Second):
		}
	}

	ticker := time.NewTicker(time.Duration(hc.Interval) * time.Second)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t, ok := m.getTask(t.ID)
			if !ok || t.State != task.Running {
				return
			}

			ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(hc.Timeout)*time.Second)
			err := m.checkTaskHealth(ctxTimeout, t, w)
			cancel() // normally i'd defer but this is in a constant for loop

			if err == nil {
				failures = 0
				continue
			}

			failures++
			log.Printf("Health check for task %s failed (%d/%d): %v\n", t.ID, failures, hc.Retries, err)
			if failures < hc.Retries {
				continue
			}

			if t.AtRestartCap(MaxRestarts) {
				reason := fmt.Sprintf("health check failed after all restarts & retries. last error: %v", err)
				log.Printf("Task %s: %s; marking failed and stopping its container\n", t.ID, reason)
				m.stopTaskTerminal(t, reason)
				return
			}

			if !task.ShouldRestart(t.RestartPolicy) {
				reason := fmt.Sprintf("health check failed (%v) and restart policy %q does not permit a restart", err, t.RestartPolicy)
				log.Printf("Task %s: %s; marking failed and stopping its container\n", t.ID, reason)
				m.stopTaskTerminal(t, reason)
				return
			}

			if time.Now().UTC().Before(t.NextRetryAt) {
				// a restart was attempted too recently. wait out the backoff
				// the task stays Running, so reconcileCheckers starts a fresh checker
				log.Printf("Task %s is within its restart backoff window; deferring the health-triggered restart\n", t.ID)
				return
			}

			log.Printf("Task %s declared unhealthy after %d consecutive failures, restarting\n", t.ID, failures)
			err = m.restartTask(t)
			if err != nil {
				log.Printf("Error restarting task %s: %v", t.ID, err)
			}
			return
		}
	}
}
