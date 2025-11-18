package flow

import (
	"sync/atomic"

	"github.com/shirou/gopsutil/v4/mem"
)

type systemMemoryReader func() (systemMemory, error)

var systemMemoryProvider atomic.Value

func init() {
	systemMemoryProvider.Store(systemMemoryReader(readSystemMemory))
}

func readSystemMemory() (systemMemory, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return systemMemory{}, err
	}

	return systemMemory{
		Total:     v.Total,
		Available: v.Available,
	}, nil
}

func getSystemMemory() (systemMemory, error) {
	return loadSystemMemoryReader()()
}

func loadSystemMemoryReader() systemMemoryReader {
	if reader := systemMemoryProvider.Load(); reader != nil {
		return reader.(systemMemoryReader)
	}
	return readSystemMemory
}

// setSystemMemoryReader replaces the current system memory reader and returns a restore function.
func setSystemMemoryReader(reader systemMemoryReader) func() {
	prev := loadSystemMemoryReader()
	systemMemoryProvider.Store(reader)
	return func() {
		systemMemoryProvider.Store(prev)
	}
}
