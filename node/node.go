package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/belyaevedu/philharmonic/stats"
	"github.com/belyaevedu/philharmonic/utils"
	"github.com/belyaevedu/philharmonic/worker"
)

var (
	StatsQueryMaxRetries  = 3
	StatsQuerySleepPeriod = 3 * time.Second
	PortsQueryTimeout     = 5 * time.Second
)

type Node struct {
	Address         string // host:port
	Cores           int    // logical CPUs reported by the worker
	Memory          int64  // total memory in KB
	Disk            int64  // total disk in bytes
	MemoryAllocated int64  // bytes reserved by the worker's running tasks
	Stats           stats.Stats
	Role            string

	mu sync.Mutex
}

func New(address, role string) (*Node, error) {
	if err := validateAddress(address); err != nil {
		return nil, fmt.Errorf("invalid node address %q: %w", address, err)
	}

	return &Node{
		Address: address,
		Role:    role,
	}, nil
}

func validateAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("must be a host:port endpoint: %w", err)
	}
	if host == "" {
		return fmt.Errorf("host must not be empty")
	}

	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("port must be an integer from 1 to 65535")
	}

	// port still can include a path if given.
	// ensuring the value remains a host, rather than becoming a path or query
	// component when it is interpolated into an HTTP URL
	endpoint, err := url.Parse("http://" + address)
	if err != nil || endpoint.Host != address {
		return fmt.Errorf("must be a valid host:port endpoint")
	}

	return nil
}

func (n *Node) GetStats() (*stats.Stats, error) {
	url := fmt.Sprintf("http://%s/stats", n.Address)
	resp, err := utils.HTTPWithRetry(httpclient.New().Get, url, StatsQueryMaxRetries, StatsQuerySleepPeriod) // #nosec G107
	if err != nil {
		return nil, fmt.Errorf("error connecting to %v: %w", n.Address, err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error retrieving stats from %v: status %d", n.Address, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading stats resp body from %v: %w", n.Address, err)
	}

	var s stats.Stats
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("error unmarshalling body of stats from %v: %w", n.Address, err)
	}

	n.mu.Lock()
	n.Cores = s.Cores
	n.Memory = clampToInt64(s.MemTotalKb())
	n.Disk = clampToInt64(s.DiskTotal())
	n.MemoryAllocated = s.MemoryAllocated
	n.Stats = s
	n.mu.Unlock()

	return &s, nil
}

func (n *Node) GetPorts() (*worker.OccupiedPorts, error) {
	url := fmt.Sprintf("http://%s/ports", n.Address)
	ctx, cancel := context.WithTimeout(context.Background(), PortsQueryTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("error building ports request for %v: %w", n.Address, err)
	}
	resp, err := httpclient.New().Do(req) // #nosec G107
	if err != nil {
		return nil, fmt.Errorf("error connecting to %v: %w", n.Address, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error retrieving ports from %v: status %d", n.Address, resp.StatusCode)
	}

	var occ worker.OccupiedPorts
	if err := json.NewDecoder(resp.Body).Decode(&occ); err != nil {
		return nil, fmt.Errorf("error unmarshalling ports from %v: %w", n.Address, err)
	}
	return &occ, nil
}

// a concurrency-safe point-in-time copy of the resource fields
type Snapshot struct {
	Cores            int
	MemoryTotalKB    int64
	MemUsedKB        int64
	MemoryAllocatedB int64
	DiskFreeB        int64
	CpuUsage         float64
	TaskCount        int
}

func (n *Node) Snapshot() Snapshot {
	n.mu.Lock() // not RLock to prevent data race
	defer n.mu.Unlock()

	return Snapshot{
		Cores:            n.Cores,
		MemoryTotalKB:    n.Memory,
		MemUsedKB:        clampToInt64(n.Stats.MemUsedKb()),
		MemoryAllocatedB: n.MemoryAllocated,
		DiskFreeB:        clampToInt64(n.Stats.DiskFree()),
		CpuUsage:         n.Stats.CpuUsage,
		TaskCount:        n.Stats.TaskCount,
	}
}

// gosec G115 fix: preventing overflow
func clampToInt64(u uint64) int64 {
	if u > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(u)
}
