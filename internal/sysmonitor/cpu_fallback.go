//go:build !linux && !windows && !darwin

package sysmonitor

// newPlatformCPUSampler returns the heuristic sampler for unsupported OSs.
func newPlatformCPUSampler(_ FileSystem) (ProcessCPUSampler, error) {
	// On unsupported platforms, we fall back to the GoroutineHeuristicSampler
	// defined in cpu_heuristic.go. This ensures the application can still
	// report an estimated "load" based on internal activity.
	return NewGoroutineHeuristicSampler(), nil
}
