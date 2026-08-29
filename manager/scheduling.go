package manager

import (
	"errors"
	"fmt"
	"log"

	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/task"
)

// worker placement: candidate selection, port admission, and failure classification

// marks an infra issue during placement
var errClusterUnreachable = errors.New("no worker is reachable")

// marks a placement attempt that ran before any stats sample was collected
var errStatsNotCollected = errors.New("worker stats have not been collected yet")

func (m *Manager) statsPending() bool {
	for _, n := range m.WorkerNodes {
		if n != nil && n.HasStats() {
			return false
		}
	}
	return true
}

func (m *Manager) clusterUnreachable() bool {
	nodes := make([]*node.Node, 0, len(m.WorkerNodes))
	for _, n := range m.WorkerNodes {
		if n != nil {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) == 0 {
		return true
	}

	// buffered so the early return on the first reachable worker
	// never leaks the remaining probe goroutines
	reachable := make(chan bool, len(nodes))
	for _, n := range nodes {
		go func(n *node.Node) {
			reachable <- n.Reachable()
		}(n)
	}
	for range nodes {
		if <-reachable {
			return false
		}
	}
	return true
}

func (m *Manager) SelectWorker(t *task.Task) (*node.Node, error) {
	m.Reservations.Lock()
	defer m.Reservations.Unlock()
	return m.selectWorkerLocked(t, "")
}

// filters candidates using a complete port inventory and the manager's reservations
// the caller must hold m.Reservations
func (m *Manager) selectWorkerLocked(t *task.Task, selfOwner string) (*node.Node, error) {
	if t == nil {
		return nil, errors.New("cannot select a worker for a nil task")
	}
	if err := task.ValidatePortMappings(t.Ports); err != nil {
		return nil, err
	}

	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available candidates match resource requests for task %s", t.ID)
	}

	able := make([]*node.Node, 0, len(candidates))
	for _, c := range candidates {
		excludeOwnPorts := selfOwner != "" && c.Address == selfOwner
		if hasPinnedHostPorts(t) {
			occ, err := m.fetchWorkerPorts(c)
			if err != nil {
				log.Printf("Skipping worker %s during port admission: %v\n", c.Address, err)
				continue
			}
			if !m.canHost(t, occ, excludeOwnPorts) {
				continue
			}
		}
		if m.Reservations.ConflictsLocked(c.Address, t, false) {
			continue
		}
		able = append(able, c)
	}
	if len(able) == 0 {
		return nil, fmt.Errorf(
			"no worker can host task %s: every candidate has a conflicting or unavailable host port",
			t.ID,
		)
	}

	scores := m.Scheduler.Score(t, able)
	selectedNode := m.Scheduler.Pick(scores, able)
	if selectedNode == nil {
		return nil, fmt.Errorf("no candidate able to host task %s (all unscoreable)", t.ID)
	}
	return selectedNode, nil
}

func (m *Manager) selectAndReserveWorker(t *task.Task, owner string) (*node.Node, error) {
	if err := task.ValidatePortMappings(t.Ports); err != nil {
		return nil, err
	}

	var (
		selected *node.Node
		selErr   error
	)
	func() { // for defer to activate in early return cases
		m.Reservations.Lock()
		defer m.Reservations.Unlock()

		if owner != "" { // restart branch
			ownerNode := m.workerByAddress(owner)
			if ownerNode != nil && !m.Reservations.ConflictsLocked(owner, t, true) {
				if !hasPinnedHostPorts(t) {
					selected = ownerNode
					return
				}
				occ, err := m.fetchWorkerPorts(ownerNode)
				if err == nil && m.canHost(t, occ, true) && m.Reservations.TryReserveLocked(owner, t) {
					selected = ownerNode
					return
				}
				if err != nil {
					log.Printf("Cannot use owning worker %s during port admission: %v\n", owner, err)
				}
			}
		}

		selected, selErr = m.selectWorkerLocked(t, "")
		if selErr != nil {
			return
		}
		// impossible while the table's lock is held and selectWorkerLocked checked
		// conflicts for every candidate. handled defensively anyway :shrug:
		if !m.Reservations.TryReserveLocked(selected.Address, t) {
			selected, selErr = nil, fmt.Errorf("lost race reserving host ports for task %s on %s", t.ID, selected.Address)
		}
	}()
	if selErr != nil {
		// have to wait for stat collection
		if m.statsPending() {
			return nil, fmt.Errorf("%w: %v", errStatsNotCollected, selErr)
		}

		if m.clusterUnreachable() {
			return nil, fmt.Errorf("%w: %v", errClusterUnreachable, selErr)
		}
		return nil, selErr
	}
	return selected, nil
}

func (m *Manager) workerByAddress(address string) *node.Node {
	for _, worker := range m.WorkerNodes {
		if worker.Address == address {
			return worker
		}
	}
	return nil
}
