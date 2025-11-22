package sysmonitor

// SystemMemory represents system memory information in bytes
type SystemMemory struct {
	Total     uint64
	Available uint64
}

// MemoryReader is a function that reads system memory information
type MemoryReader func() (SystemMemory, error)
