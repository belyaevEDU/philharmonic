package worker

import "github.com/c9s/goprocinfo/linux"

type Stats struct {
	MemStats  *linux.MemInfo
	DiskStats *linux.Disk
	CpuStats  *linux.CPUStat
	LoadStats *linux.LoadAvg
}

// ram-related helpers

func (s *Stats) MemTotalKb() uint64 {
	return s.MemStats.MemTotal
}

func (s *Stats) MemAvailableKb() uint64 {
	return s.MemStats.MemAvailable
}

func (s *Stats) MemUsedKb() uint64 {
	return s.MemTotalKb() - s.MemAvailableKb()
}

func (s *Stats) MemUsedPercent() uint64 {
	return s.MemAvailableKb() / s.MemTotalKb()
}

// disk-related helpers

func (s *Stats) DiskTotal() uint64 {
	return s.DiskStats.All
}

func (s *Stats) DiskFree() uint64 {
	return s.DiskStats.Free
}

func (s *Stats) DiskUsed() uint64 {
	return s.DiskStats.Used
}

// cpu-related helpers

func (s *Stats) CpuUsage() float64 {
	// https://stackoverflow.com/questions/23367857/accurate-calculation-of-cpu-usage-given-in-percentage-in-linux
	// lord almighty...
	idle := s.CpuStats.Idle + s.CpuStats.IOWait
	nonIdle := s.CpuStats.User + s.CpuStats.Nice + s.CpuStats.System +
		s.CpuStats.IRQ + s.CpuStats.SoftIRQ + s.CpuStats.Steal
	total := idle + nonIdle

	if total == 0 {
		return 0.00
	}

	return (float64(total) - float64(idle)) / float64(total)
}
