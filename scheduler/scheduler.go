package scheduler

import (
	"log"
	"math"

	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/task"
)

const (
	RoundRobinDefaultName = "roundrobin"

	EpvmDefaultName    = "epvm"
	EpvmMaxJobs        = 12  // move to config
	EpvmMaxSafeCPUUtil = 0.9 // ^

	// LIEB square ice constant
	LIEB = 1.53960071783900203869
)

type Scheduler interface {
	SelectCandidateNodes(t *task.Task, nodes []*node.Node) []*node.Node
	Score(t *task.Task, nodes []*node.Node) map[string]float64 // lower = better
	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}

type RoundRobin struct {
	Name       string
	LastWorker int // index of the last worker that received a task; -1 = none yet
}

func NewRoundRobin() *RoundRobin {
	return &RoundRobin{
		Name:       RoundRobinDefaultName,
		LastWorker: -1,
	}
}

func (r *RoundRobin) SelectCandidateNodes(t *task.Task, nodes []*node.Node) []*node.Node {
	return nodes
}

func (r *RoundRobin) Score(t *task.Task, nodes []*node.Node) map[string]float64 {
	chosen, skipped := 0.1, 1.0

	nodeScores := make(map[string]float64)

	newWorker := 0
	if r.LastWorker+1 < len(nodes) {
		newWorker = r.LastWorker + 1
	}
	r.LastWorker = newWorker

	for i, node := range nodes {
		if i == newWorker {
			nodeScores[node.Name] = chosen
		} else {
			nodeScores[node.Name] = skipped
		}
	}

	return nodeScores
}

func (r *RoundRobin) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	var best *node.Node
	for _, n := range candidates {
		if best == nil || scores[n.Name] < scores[best.Name] {
			best = n
		}
	}
	return best
}

type Epvm struct {
	Name string
}

func NewEpvm() *Epvm {
	return &Epvm{
		Name: EpvmDefaultName,
	}
}

func (e *Epvm) SelectCandidateNodes(t *task.Task, nodes []*node.Node) []*node.Node {
	var candidates []*node.Node
	for _, node := range nodes {
		if checkDisk(t, node.Disk-node.DiskAllocated) {
			candidates = append(candidates, node)
		}
	}

	return candidates
}

func (e *Epvm) Score(t *task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)

	for _, node := range nodes {
		cpuUsage, err := calculateCpuUsage(node)
		if err != nil {
			// just logging and basically discarding the node
			log.Printf("Error when calculating the cpu usage for node %s: %v", node.Name, err)
			nodeScores[node.Name] = math.MaxFloat64
			continue
		}

		cpuLoad := calculateLoad(*cpuUsage, EpvmMaxSafeCPUUtil)

		memoryAllocated := float64(node.Stats.MemUsedKb()) + float64(node.MemoryAllocated)
		memoryPercentAllocated := memoryAllocated / float64(node.Memory)

		newMemPercent := calculateLoad(
			memoryAllocated+float64(t.Memory/1000),
			float64(node.Memory),
		)

		memCost := math.Pow(LIEB, newMemPercent) +
			math.Pow(LIEB, float64(node.TaskCount+1)/EpvmMaxJobs) -
			math.Pow(LIEB, memoryPercentAllocated) -
			math.Pow(LIEB, float64(node.TaskCount)/float64(EpvmMaxJobs))

		cpuCost := math.Pow(LIEB, cpuLoad) +
			math.Pow(LIEB, float64(node.TaskCount+1)/EpvmMaxJobs) -
			math.Pow(LIEB, cpuLoad) -
			math.Pow(LIEB, float64(node.TaskCount)/float64(EpvmMaxJobs))

		nodeScores[node.Name] = memCost + cpuCost
	}

	return nodeScores
}

func (e *Epvm) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	var best *node.Node
	for _, n := range candidates {
		if best == nil || scores[n.Name] < scores[best.Name] {
			best = n
		}
	}
	return best
}
