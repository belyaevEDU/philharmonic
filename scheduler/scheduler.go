package scheduler

import (
	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/task"
)

type Scheduler interface {
	SelectCandidateNodes(t *task.Task, nodes []*node.Node) []*node.Node
	Score(t *task.Task, nodes []*node.Node) map[string]float64 // lower = better
	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}

type RoundRobin struct {
	Name       string
	LastWorker int
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
