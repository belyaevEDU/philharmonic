package manager

import (
	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/belyaevedu/philharmonic/worker"
)

func hasPinnedHostPorts(t *task.Task) bool {
	if t == nil {
		return false
	}
	for _, pm := range t.Ports {
		if pm.HostPort != 0 {
			return true
		}
	}
	return false
}

func (m *Manager) canHost(t *task.Task, occ *worker.OccupiedPorts, excludeOwnPorts bool) bool {
	if !hasPinnedHostPorts(t) {
		return true
	}
	if occ == nil {
		return false
	}

	occupied := make(map[string]struct{}, len(occ.TCP)+len(occ.UDP))
	for _, p := range occ.TCP {
		occupied[protoPortKey("tcp", p)] = struct{}{}
	}
	for _, p := range occ.UDP {
		occupied[protoPortKey("udp", p)] = struct{}{}
	}

	if excludeOwnPorts {
		for _, pm := range t.HostPorts {
			if pm.HostPort != 0 {
				delete(occupied, protoPortKey(string(pm.Protocol), pm.HostPort))
			}
		}
	}

	for _, pm := range t.Ports {
		if pm.HostPort == 0 {
			continue
		}
		if _, hit := occupied[protoPortKey(string(pm.Protocol), pm.HostPort)]; hit {
			return false
		}
	}
	return true
}

func (m *Manager) fetchWorkerPorts(n *node.Node) (*worker.OccupiedPorts, error) {
	return n.GetPorts()
}
