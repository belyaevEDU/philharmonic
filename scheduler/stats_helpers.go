package scheduler

import (
	"time"

	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/task"
)

func checkDisk(t *task.Task, diskAvailable int64) bool {
	return t.Disk <= diskAvailable
}

func calculateLoad(usage, capacity float64) float64 {
	return usage / capacity
}

func calculateCpuUsage(node *node.Node) (*float64, error) {
	// not using the Stats.CpuUsage method
	// because the calculation is a bit different

	stat1, err := node.GetStats()
	if err != nil {
		return nil, err
	}

	time.Sleep(3 * time.Second)

	stat2, err := node.GetStats()
	if err != nil {
		return nil, err
	}

	stat1Idle, stat1NonIdle := stat1.CpuUsageSplit()
	stat2Idle, stat2NonIdle := stat2.CpuUsageSplit()

	stat1Total := stat1Idle + stat1NonIdle
	stat2Total := stat2Idle + stat2NonIdle

	total := stat2Total - stat1Total
	idle := stat2Idle - stat1Idle

	var cpuPercentUsage float64
	if total == 0 && idle == 0 {
		cpuPercentUsage = 0.00
	} else {
		cpuPercentUsage = (float64(total) - float64(idle)) / float64(total)
	}
	return &cpuPercentUsage, nil
}
