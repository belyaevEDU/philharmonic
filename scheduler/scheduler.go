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
	epvmMaxJobs        = 12  // move to config
	epvmMaxSafeCPUUtil = 0.9 // ^

	// LIEB square ice constant
	lieb = 1.53960071783900203869

	// task.Memory/Disk are in bytes while memory stats are in KB
	bytesPerKB = 1024.0
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
			nodeScores[node.Address] = chosen
		} else {
			nodeScores[node.Address] = skipped
		}
	}

	return nodeScores
}

func (r *RoundRobin) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	var best *node.Node
	for _, n := range candidates {
		if best == nil || scores[n.Address] < scores[best.Address] {
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
	for _, n := range nodes {
		if _, err := n.GetStats(); err != nil {
			log.Printf("Error fetching stats for node %s: %v", n.Address, err)
			continue
		}
		if checkDisk(t, n.Snapshot().DiskFreeB) {
			candidates = append(candidates, n)
		}
	}

	return candidates
}

func (e *Epvm) Score(t *task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64, len(nodes))

	for _, n := range nodes {
		snap := n.Snapshot()

		if snap.MemoryTotalKB <= 0 {
			// unknown capacity, so can't reason about cost, skip
			nodeScores[n.Address] = math.MaxFloat64
			continue
		}

		jobsNow := float64(snap.TaskCount)
		jobsNext := float64(snap.TaskCount + 1)

		baseMemKB := float64(snap.MemUsedKB)
		projectedMemKB := baseMemKB + float64(t.Memory)/bytesPerKB
		memPercent := calculateLoad(baseMemKB, float64(snap.MemoryTotalKB))
		newMemPercent := calculateLoad(projectedMemKB, float64(snap.MemoryTotalKB))

		// cpu: current utilization vs. with this task's share added
		var cpuShare float64
		if snap.Cores > 0 {
			cpuShare = t.Cpu / float64(snap.Cores)
		}
		cpuLoad := calculateLoad(snap.CpuUsage, epvmMaxSafeCPUUtil)
		newCpuLoad := cpuLoad + calculateLoad(cpuShare, epvmMaxSafeCPUUtil)

		memCost := math.Pow(lieb, newMemPercent) +
			math.Pow(lieb, jobsNext/float64(epvmMaxJobs)) -
			math.Pow(lieb, memPercent) -
			math.Pow(lieb, jobsNow/float64(epvmMaxJobs))

		cpuCost := math.Pow(lieb, newCpuLoad) +
			math.Pow(lieb, jobsNext/float64(epvmMaxJobs)) -
			math.Pow(lieb, cpuLoad) -
			math.Pow(lieb, jobsNow/float64(epvmMaxJobs))

		nodeScores[n.Address] = memCost + cpuCost
	}

	return nodeScores
}

func (e *Epvm) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	var best *node.Node
	for _, n := range candidates {
		score := scores[n.Address]
		if score == math.MaxFloat64 {
			continue
		}
		if best == nil || score < scores[best.Address] {
			best = n
		}
	}
	return best
}
