package sysmonitor

// SystemMemory represents system memory information in bytes.
type SystemMemory struct {
	Total     uint64
	Available uint64
}

type ProcessMemoryReader interface {
	Sample() (SystemMemory, error)
}

// NewProcessMemoryReader creates a new process memory reader.
func NewProcessMemoryReader(fs FileSystem) ProcessMemoryReader {
	return newPlatformMemoryReader(fs)
}
