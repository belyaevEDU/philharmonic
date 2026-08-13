package node

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

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
	Cores           int
	Memory          int64
	MemoryAllocated int64
	Disk            int64
	DiskAllocated   int64
	Stats           stats.Stats
	Role            string
	TaskCount       int
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

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("error retrieving stats from %v: %w", n.Api, err)
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

	var stats stats.Stats
	if err := json.Unmarshal(body, &stats); err != nil {
		return nil, fmt.Errorf("error unmarshalling body of stats from %v: %w", n.Api, err)
	}

	n.Memory = int64(stats.MemTotalKb())
	n.Disk = int64(stats.DiskTotal())

	n.Stats = stats

	return &stats, nil
}
