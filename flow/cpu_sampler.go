package flow

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/shirou/gopsutil/v4/process"
)

type gopsutilProcessSampler struct {
	proc        *process.Process
	lastPercent float64
	lastSample  time.Time
}

func newGopsutilProcessSampler() (*gopsutilProcessSampler, error) {
	pid := os.Getpid()
	if pid < 0 || pid > math.MaxInt32 {
		return nil, fmt.Errorf("invalid PID: %d", pid)
	}
	proc, err := process.NewProcess(int32(pid))
	if err != nil {
		return nil, err
	}

	return &gopsutilProcessSampler{
		proc: proc,
	}, nil
}

func (s *gopsutilProcessSampler) Sample(deltaTime time.Duration) float64 {
	percent, err := s.proc.CPUPercent()
	if err != nil {
		return (&goroutineHeuristicSampler{}).Sample(deltaTime)
	}

	now := time.Now()
	if s.lastSample.IsZero() {
		s.lastSample = now
		s.lastPercent = percent
		return 0.0
	}

	elapsed := now.Sub(s.lastSample)
	if elapsed < deltaTime/2 {
		return s.lastPercent
	}

	s.lastPercent = percent
	s.lastSample = now

	if percent > 100.0 {
		percent = 100.0
	} else if percent < 0.0 {
		percent = 0.0
	}

	return percent
}

func (s *gopsutilProcessSampler) Reset() {
	s.lastPercent = 0
	s.lastSample = time.Time{}
}

func (s *gopsutilProcessSampler) IsInitialized() bool {
	return !s.lastSample.IsZero()
}
