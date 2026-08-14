package node

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sync"

	"github.com/belyaevedu/philharmonic/stats"
	"github.com/belyaevedu/philharmonic/utils"
)

const (
	StatsQueryMaxRetries  = 5
	StatsQuerySleepPeriod = 5
)

type Node struct {
	Name            string // ip:port
	Api             string
	Cores           int   // logical CPUs reported by the worker
	Memory          int64 // total memory in KB
	Disk            int64 // total disk in bytes
	MemoryAllocated int64 // bytes reserved by the worker's running tasks
	Stats           stats.Stats
	Role            string

	mu sync.Mutex
}

func NewNode(name, api, role string) *Node {
	return &Node{
		Name: name,
		Api:  api,
		Role: role,
	}
}

func (n *Node) GetStats() (*stats.Stats, error) {
	url := fmt.Sprintf("%s/stats", n.Api)
	resp, err := utils.HTTPWithRetry(http.Get, url, StatsQueryMaxRetries, StatsQuerySleepPeriod) // #nosec G107
	if err != nil {
		return nil, fmt.Errorf("error connecting to %v: %w", n.Api, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error retrieving stats from %v: status %d", n.Api, resp.StatusCode)
	}

	defer func() {
		err := resp.Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading stats resp body from %v: %w", n.Api, err)
	}

	var s stats.Stats
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("error unmarshalling body of stats from %v: %w", n.Api, err)
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
