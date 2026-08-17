package manager

import (
	"strconv"
	"strings"

	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/belyaevedu/philharmonic/worker"
	"github.com/google/uuid"
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

func protoPortKey(proto string, port int) string {
	if proto == "" {
		proto = "tcp"
	}
	return strings.ToLower(proto) + ":" + strconv.Itoa(port)
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

func requestedPortKeys(t *task.Task) []string {
	if t == nil {
		return nil
	}
	keys := make([]string, 0, len(t.Ports))
	for _, pm := range t.Ports {
		if pm.HostPort != 0 {
			keys = append(keys, protoPortKey(string(pm.Protocol), pm.HostPort))
		}
	}
	return keys
}

func (m *Manager) portReservationConflictLocked(workerAddress string, t *task.Task, allowOwn bool) bool {
	reservations := m.portReservations[workerAddress]
	for _, key := range requestedPortKeys(t) {
		if owner, exists := reservations[key]; exists && (owner != t.ID || !allowOwn) {
			return true
		}
	}
	return false
}

func (m *Manager) reservePortsLocked(workerAddress string, t *task.Task) {
	keys := requestedPortKeys(t)
	if len(keys) == 0 {
		return
	}
	if m.portReservations == nil {
		m.portReservations = make(map[string]map[string]uuid.UUID)
	}
	reservations := m.portReservations[workerAddress]
	if reservations == nil {
		reservations = make(map[string]uuid.UUID)
		m.portReservations[workerAddress] = reservations
	}
	for _, key := range keys {
		reservations[key] = t.ID
	}
}

func (m *Manager) reservePorts(workerAddress string, t *task.Task) bool {
	m.portMu.Lock()
	defer m.portMu.Unlock()
	if m.portReservationConflictLocked(workerAddress, t, true) {
		return false
	}
	m.reservePortsLocked(workerAddress, t)
	return true
}

func (m *Manager) releasePorts(workerAddress string, t *task.Task) {
	m.portMu.Lock()
	defer m.portMu.Unlock()
	reservations := m.portReservations[workerAddress]
	for _, key := range requestedPortKeys(t) {
		if owner, exists := reservations[key]; exists && owner == t.ID {
			delete(reservations, key)
		}
	}
	if len(reservations) == 0 {
		delete(m.portReservations, workerAddress)
	}
}
