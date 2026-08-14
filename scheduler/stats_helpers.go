package scheduler

import (
	"github.com/belyaevedu/philharmonic/task"
)

func checkDisk(t *task.Task, diskAvailable int64) bool {
	return t.Disk >= 0 && t.Disk <= diskAvailable
}

func calculateLoad(usage, capacity float64) float64 {
	if capacity <= 0 {
		return 0
	}
	return usage / capacity
}
