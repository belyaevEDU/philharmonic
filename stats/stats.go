package stats

import (
	"log"

	"github.com/c9s/goprocinfo/linux"
)

type Stats struct {
	MemStats  *linux.MemInfo
	DiskStats *linux.Disk
	CpuStats  *linux.CPUStat
	LoadStats *linux.LoadAvg

	// brought these out into their own fields since they are reported by the worker
	TaskCount       int
	CpuUsage        float64 // [0, 1]
	Cores           int
	MemoryAllocated int64 // bytes reserved by the worker's running tasks
}

func GetStats() *Stats {
	return &Stats{
		MemStats:  GetMemoryInfo(),
		DiskStats: GetDiskInfo(),
		CpuStats:  GetCpuStats(),
		LoadStats: GetLoadAvg(),
	}
}

// ram-related helpers

func (s *Stats) MemTotalKb() uint64 {
	if s.MemStats == nil {
		return 0
	}
	return s.MemStats.MemTotal
}

func (s *Stats) MemAvailableKb() uint64 {
	if s.MemStats == nil {
		return 0
	}
	return s.MemStats.MemAvailable
}

func (s *Stats) MemUsedKb() uint64 {
	total := s.MemTotalKb()
	avail := s.MemAvailableKb()
	if avail >= total {
		return 0
	}
	return total - avail
}

func (s *Stats) MemUsedPercent() uint64 {
	total := s.MemTotalKb()
	if total == 0 {
		return 0
	}
	return s.MemAvailableKb() / total
}

// disk-related helpers

func (s *Stats) DiskTotal() uint64 {
	if s.DiskStats == nil {
		return 0
	}
	return s.DiskStats.All
}

func (s *Stats) DiskFree() uint64 {
	if s.DiskStats == nil {
		return 0
	}
	return s.DiskStats.Free
}

func (s *Stats) DiskUsed() uint64 {
	if s.DiskStats == nil {
		return 0
	}
	return s.DiskStats.Used
}

// cpu-related helpers

func (s *Stats) CpuUsageSplit() (idle uint64, nonIdle uint64) {
	if s.CpuStats == nil {
		return 0, 0
	}
	// https://stackoverflow.com/questions/23367857/accurate-calculation-of-cpu-usage-given-in-percentage-in-linux
	// lord almighty...
	idle = s.CpuStats.Idle + s.CpuStats.IOWait
	nonIdle = s.CpuStats.User + s.CpuStats.Nice + s.CpuStats.System +
		s.CpuStats.IRQ + s.CpuStats.SoftIRQ + s.CpuStats.Steal

	return idle, nonIdle
}

// helpers for pulling info from the proc fs

func GetMemoryInfo() *linux.MemInfo {
	memStats, err := linux.ReadMemInfo("/proc/meminfo")
	if err != nil {
		log.Println("Error reading from /proc/meminfo")
		return &linux.MemInfo{}
	}

	return memStats
}

func GetDiskInfo() *linux.Disk {
	diskStats, err := linux.ReadDisk("/")
	if err != nil {
		log.Println("Error reading from /")
		return &linux.Disk{}
	}

	return diskStats
}

func GetCpuStats() *linux.CPUStat {
	stats, err := linux.ReadStat("/proc/stat")
	if err != nil {
		log.Println("Error reading from /proc/stat")
		return &linux.CPUStat{}
	}

	return &stats.CPUStatAll
}

func GetLoadAvg() *linux.LoadAvg {
	loadAvg, err := linux.ReadLoadAvg("/proc/loadavg")
	if err != nil {
		log.Println("Error reading from /proc/loadavg")
		return &linux.LoadAvg{}
	}

	return loadAvg
}
