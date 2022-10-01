//go:build !windows
// +build !windows

package ospkg

const (
	// PathSeparator is a path separator string for a Unix-like os
	PathSeparator = "/"

	// NewLine is a new line constant for a Unix-like os
	NewLine = "\n"
)
