package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/belyaevedu/philharmonic/handlers"
	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/belyaevedu/philharmonic/worker"
)

// cluster-wide image pulls

// resolves the target list for a cluster-wide image pull.
// an empty, omitted or all-blank names list selects every configured worker.
// each name must equal a configured worker address.
// duplicates and blank entries are dropped.
// returns: the targets, the names matching no configured worker,
// and the full configured list for error messages
func (m *Manager) resolvePullTargets(names []string) (targets, unknown, known []string) {
	m.mu.RLock()
	known = make([]string, 0, len(m.WorkerNodes))
	for _, n := range m.WorkerNodes {
		if n != nil {
			known = append(known, n.Address)
		}
	}
	m.mu.RUnlock()

	if len(names) == 0 {
		return known, nil, known
	}

	configured := make(map[string]struct{}, len(known))
	for _, addr := range known {
		configured[addr] = struct{}{}
	}

	targets = make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		if _, ok := configured[name]; ok {
			targets = append(targets, name)
		} else {
			unknown = append(unknown, name)
		}
	}

	// every entry was blank,
	// so fall back to selecting every configured worker, mirroring an omitted list
	if len(targets) == 0 && len(unknown) == 0 {
		return known, nil, known
	}
	return targets, unknown, known
}

// asks a worker to pull image onto its docker daemon
func (m *Manager) pullImageOnWorker(workerAddr, image string) (bool, error) {
	data, err := json.Marshal(worker.PullImageRequest{Image: image})
	if err != nil {
		return false, fmt.Errorf("error marshalling pull request: %w", err)
	}

	url := httpclient.WorkerURL(workerAddr, "/images")
	resp, err := httpclient.WorkerLongOp().Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
	if err != nil {
		return false, fmt.Errorf("error connecting to worker %s: %w", workerAddr, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body from worker %s: %v\n", workerAddr, err)
		}
	}()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProxyLogSize))
	if err != nil {
		return false, fmt.Errorf("error reading response body from worker %s: %w", workerAddr, err)
	}

	if resp.StatusCode != http.StatusOK {
		hr := handlers.HTTPResponse{}
		reason := strings.TrimSpace(string(body))
		if json.Unmarshal(body, &hr) == nil && hr.Message != "" {
			reason = hr.Message
		}
		return false, fmt.Errorf("worker %s responded with status %d: %s", workerAddr, resp.StatusCode, reason)
	}

	pullResp := worker.PullImageResponse{}
	if err := json.Unmarshal(body, &pullResp); err != nil {
		return false, fmt.Errorf("error unmarshalling response from worker %s: %w", workerAddr, err)
	}
	return pullResp.Pulled, nil
}
