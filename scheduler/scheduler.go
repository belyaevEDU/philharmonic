package scheduler

import (
	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/task"
)

type Scheduler interface {
	SelectCandidateNodes(t *task.Task, nodes []*node.Node) []*node.Node
	Score(t *task.Task, nodes []*node.Node) map[string]float64
	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}
