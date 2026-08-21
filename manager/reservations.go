package manager

import (
	"strconv"
	"strings"
	"sync"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/google/uuid"
)

type ReservationStore interface {
	Lock()
	Unlock()

	TryReserve(workerAddress string, t *task.Task) bool
	Release(workerAddress string, t *task.Task)
	Conflicts(workerAddress string, t *task.Task, allowOwn bool) bool
	ConflictsLocked(workerAddress string, t *task.Task, allowOwn bool) bool
	TryReserveLocked(workerAddress string, t *task.Task) bool
}

// per worker, a map from "proto:port" to the owning task ID
type ReservationTable struct {
	mu       sync.Mutex
	byWorker map[string]map[string]uuid.UUID
}

func NewReservationTable() *ReservationTable {
	return &ReservationTable{byWorker: make(map[string]map[string]uuid.UUID)}
}

func (rt *ReservationTable) Lock() { rt.mu.Lock() }
func (rt *ReservationTable) Unlock() {
	rt.mu.Unlock()
}

func (rt *ReservationTable) TryReserve(workerAddress string, t *task.Task) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.tryReserveLocked(workerAddress, t)
}

func (rt *ReservationTable) TryReserveLocked(workerAddress string, t *task.Task) bool {
	return rt.tryReserveLocked(workerAddress, t)
}

// caller must hold rt.mu
func (rt *ReservationTable) tryReserveLocked(workerAddress string, t *task.Task) bool {
	keys := requestedPortKeys(t)
	if len(keys) == 0 {
		return true // nothing pinned, nothing to claim
	}
	if rt.conflictsLocked(workerAddress, t, true) {
		return false
	}
	reservations := rt.byWorker[workerAddress]
	if reservations == nil {
		reservations = make(map[string]uuid.UUID)
		rt.byWorker[workerAddress] = reservations
	}
	for _, key := range keys {
		reservations[key] = t.ID
	}
	return true
}

func (rt *ReservationTable) Release(workerAddress string, t *task.Task) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	reservations := rt.byWorker[workerAddress]
	for _, key := range requestedPortKeys(t) {
		if owner, exists := reservations[key]; exists && owner == t.ID {
			delete(reservations, key)
		}
	}
	if len(reservations) == 0 {
		delete(rt.byWorker, workerAddress)
	}
}

func (rt *ReservationTable) Conflicts(workerAddress string, t *task.Task, allowOwn bool) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.conflictsLocked(workerAddress, t, allowOwn)
}

func (rt *ReservationTable) ConflictsLocked(workerAddress string, t *task.Task, allowOwn bool) bool {
	return rt.conflictsLocked(workerAddress, t, allowOwn)
}

// caller must hold rt.mu
func (rt *ReservationTable) conflictsLocked(workerAddress string, t *task.Task, allowOwn bool) bool {
	reservations := rt.byWorker[workerAddress]
	for _, key := range requestedPortKeys(t) {
		if owner, exists := reservations[key]; exists && (owner != t.ID || !allowOwn) {
			return true
		}
	}
	return false
}

var _ ReservationStore = (*ReservationTable)(nil)

func protoPortKey(proto string, port int) string {
	if proto == "" {
		proto = "tcp"
	}
	return strings.ToLower(proto) + ":" + strconv.Itoa(port)
}

// the pinned ("hostPort != 0") port keys a task would claim on a worker
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
