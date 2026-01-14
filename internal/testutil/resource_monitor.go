package testutil

import (
	"time"
)

const (
	// TestSampleInterval is the default sample interval for testing.
	TestSampleInterval = 10 * time.Millisecond
	// TestStopTimeout is the timeout used when testing stop behavior.
	TestStopTimeout = 50 * time.Millisecond
	// TestStatsUpdateMargin is the margin allowed for stats update timing in tests.
	TestStatsUpdateMargin = 20 * time.Millisecond
)
