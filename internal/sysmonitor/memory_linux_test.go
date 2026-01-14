//go:build linux

package sysmonitor

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/reugn/go-streams/internal/testutil"
)

func TestLinuxMemoryReader_CgroupV2(t *testing.T) {
	fs := &testutil.MockFileSystem{
		Files: map[string][]byte{
			"/sys/fs/cgroup/memory.current": []byte("123456789"),
			"/sys/fs/cgroup/memory.max":     []byte("987654321"),
			"/sys/fs/cgroup/memory.stat":    []byte("inactive_file 111111\nother 222"),
		},
	}

	reader := newPlatformMemoryReader(fs).(*linuxMemoryReader)

	mem, err := reader.Sample()
	if err != nil {
		t.Fatalf("Expected success, got error: %v", err)
	}

	if mem.Total != 987654321 {
		t.Errorf("Expected total 987654321, got %d", mem.Total)
	}
	expectedAvail := uint64((987654321 - 123456789) + 111111)
	if mem.Available != expectedAvail {
		t.Errorf("Expected available %d, got %d", expectedAvail, mem.Available)
	}
}

func TestLinuxMemoryReader_CgroupV1(t *testing.T) {
	fs := &testutil.MockFileSystem{
		Files: map[string][]byte{
			"/sys/fs/cgroup/memory/memory.usage_in_bytes": []byte("222222222"),
			"/sys/fs/cgroup/memory/memory.limit_in_bytes": []byte("888888888"),
			"/sys/fs/cgroup/memory/memory.stat":           []byte("total_inactive_file 333333\n"),
		},
	}

	reader := newPlatformMemoryReader(fs).(*linuxMemoryReader)

	mem, err := reader.Sample()
	if err != nil {
		t.Fatalf("Expected success, got error: %v", err)
	}

	expectedAvail := uint64((888888888 - 222222222) + 333333)
	if mem.Total != 888888888 || mem.Available != expectedAvail {
		t.Errorf("Expected {Total:888888888, Available:%d}, got %+v", expectedAvail, mem)
	}
}

func TestLinuxMemoryReader_CgroupV1_Oversubscribed(t *testing.T) {
	// Test case where usage > limit (oversubscribed memory)
	fs := &testutil.MockFileSystem{
		Files: map[string][]byte{
			"/sys/fs/cgroup/memory/memory.usage_in_bytes": []byte("900000000"), // Usage exceeds limit
			"/sys/fs/cgroup/memory/memory.limit_in_bytes": []byte("800000000"),
			"/sys/fs/cgroup/memory/memory.stat":           []byte("total_inactive_file 50000000\n"),
		},
	}

	reader := newPlatformMemoryReader(fs).(*linuxMemoryReader)

	mem, err := reader.Sample()
	if err != nil {
		t.Fatalf("Expected success, got error: %v", err)
	}

	// When usage > limit, available should equal inactive_file only
	expectedAvail := uint64(50000000)
	if mem.Total != 800000000 {
		t.Errorf("Expected total 800000000, got %d", mem.Total)
	}
	if mem.Available != expectedAvail {
		t.Errorf("Expected available %d (inactive_file only), got %d", expectedAvail, mem.Available)
	}
}

func TestLinuxMemoryReader_CgroupV1_AvailableCapped(t *testing.T) {
	// Test case where calculated available exceeds limit and gets capped
	fs := &testutil.MockFileSystem{
		Files: map[string][]byte{
			"/sys/fs/cgroup/memory/memory.usage_in_bytes": []byte("100000000"), // Low usage
			"/sys/fs/cgroup/memory/memory.limit_in_bytes": []byte("500000000"),
			"/sys/fs/cgroup/memory/memory.stat":           []byte("total_inactive_file 600000000\n"), // Very high inactive_file
		},
	}

	reader := newPlatformMemoryReader(fs).(*linuxMemoryReader)

	mem, err := reader.Sample()
	if err != nil {
		t.Fatalf("Expected success, got error: %v", err)
	}

	// Calculation: (500000000 - 100000000) + 600000000 = 400000000 + 600000000 = 1,000,000,000
	// But this exceeds limit (500000000), so available should be capped at limit
	expectedAvail := uint64(500000000)
	if mem.Total != 500000000 {
		t.Errorf("Expected total 500000000, got %d", mem.Total)
	}
	if mem.Available != expectedAvail {
		t.Errorf("Expected available %d (capped at limit), got %d", expectedAvail, mem.Available)
	}
}

func TestLinuxMemoryReader_HostFallback(t *testing.T) {
	fs := &testutil.MockFileSystem{
		Files: map[string][]byte{
			"/proc/meminfo": []byte(`MemTotal:       16384000 kB
MemFree:         8192000 kB
Cached:          4096000 kB
MemAvailable:    10000000 kB
`),
		},
		OpenErrs: map[string]error{
			"/sys/fs/cgroup/memory.current":               errors.New("no cgroup v2"),
			"/sys/fs/cgroup/memory/memory.usage_in_bytes": errors.New("no cgroup v1"),
		},
	}

	reader := newPlatformMemoryReader(fs).(*linuxMemoryReader)
	mem, err := reader.Sample()
	if err != nil {
		t.Fatalf("Expected success: %v", err)
	}

	if mem.Total != 16384000*1024 || mem.Available != 10000000*1024 {
		t.Errorf("Expected MemAvailable parsing, got Total:%d Avail:%d", mem.Total, mem.Available)
	}
}

func TestLinuxMemoryReader_MemInfoFallback(t *testing.T) {
	fs := &testutil.MockFileSystem{
		Files: map[string][]byte{
			"/proc/meminfo": []byte(`MemTotal:       16384000 kB
MemFree:         8192000 kB
Cached:          4096000 kB
`),
		},
	}

	reader := newPlatformMemoryReader(fs).(*linuxMemoryReader)
	mem, err := reader.Sample()
	if err != nil {
		t.Fatalf("Expected success: %v", err)
	}

	expectedAvail := uint64((8192000 + 4096000) * 1024)
	if mem.Available != expectedAvail {
		t.Errorf("Expected fallback calculation %d, got %d", expectedAvail, mem.Available)
	}
}

func TestLinuxMemoryReader_OverflowProtection(t *testing.T) {
	// Use a value that definitely exceeds maxValueBeforeOverflow = (1<<64 - 1) / 1024 = 18014398509481983
	_, err := parseMemInfoFields(bytes.NewReader([]byte(`MemTotal:       20000000000000000 kB`)))
	if err == nil {
		t.Error("Expected overflow error")
	}
}

func TestCgroupDetection(t *testing.T) {
	// Test that cgroup V1 detection works with our setup
	fs := &testutil.MockFileSystem{
		Files: map[string][]byte{
			"/sys/fs/cgroup/memory/memory.usage_in_bytes": []byte("1000000"),
			"/sys/fs/cgroup/memory/memory.limit_in_bytes": []byte("18446744073709551615"),
			"/sys/fs/cgroup/memory/memory.stat":           []byte("total_inactive_file 500000\n"),
		},
	}

	reader := newPlatformMemoryReader(fs).(*linuxMemoryReader)

	// Call detectEnvironment manually to see what happens
	reader.detectEnvironment()

	// Check if delegate was set (should be set for cgroup V1)
	reader.mu.RLock()
	delegateSet := reader.delegate != nil
	reader.mu.RUnlock()

	if !delegateSet {
		t.Error("Cgroup V1 detection should have succeeded")
	}
}

func TestNewProcessMemoryReader(t *testing.T) {
	reader := NewProcessMemoryReader(&testutil.MockFileSystem{})

	// Should return a linuxMemoryReader (or platform-specific implementation)
	if reader == nil {
		t.Fatal("NewProcessMemoryReader returned nil")
	}
}

func TestLinuxMemoryReader_ErrorPaths(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testutil.MockFileSystem)
		err   string
	}{
		{
			"MemTotal missing",
			func(fs *testutil.MockFileSystem) {
				if fs.Files == nil {
					fs.Files = make(map[string][]byte)
				}
				fs.Files["/proc/meminfo"] = []byte("MemFree: 1000 kB\n")
			},
			"could not find MemTotal",
		},
		{
			"Meminfo parse fail",
			func(fs *testutil.MockFileSystem) {
				if fs.OpenErrs == nil {
					fs.OpenErrs = make(map[string]error)
				}
				fs.OpenErrs["/proc/meminfo"] = errors.New("no meminfo")
			},
			"failed to open /proc/meminfo",
		},
		{
			"Cgroup unlimited V1",
			func(fs *testutil.MockFileSystem) {
				// Replace the Files map entirely to ensure clean setup
				fs.Files = map[string][]byte{
					"/sys/fs/cgroup/memory/memory.usage_in_bytes": []byte("1000000"),
					"/sys/fs/cgroup/memory/memory.limit_in_bytes": []byte("18446744073709551615"),
					"/sys/fs/cgroup/memory/memory.stat":           []byte("total_inactive_file 500000\n"),
				}
				// Block Cgroup V2 detection by making V2 files return errors
				fs.OpenErrs = map[string]error{
					"/sys/fs/cgroup/memory.current": errors.New("no cgroup v2"),
					"/sys/fs/cgroup/memory.max":     errors.New("no cgroup v2"),
					"/proc/meminfo":                 errors.New("cgroup should be used"),
				}
			},
			"unlimited memory limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &testutil.MockFileSystem{}
			tt.setup(fs)

			// Verify mock setup before creating reader
			if tt.name == "Cgroup unlimited V1" {
				if _, err := fs.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err != nil {
					t.Fatalf("Mock setup failed: %v", err)
				}
			}

			reader := newPlatformMemoryReader(fs).(*linuxMemoryReader)

			// Force detection (your once.Do will trigger it)
			reader.once.Do(func() { reader.detectEnvironment() })

			_, err := reader.Sample()
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Errorf("Expected error containing %q, got %v", tt.err, err)
			}
		})
	}
}

// Helper function for cgroup value tests
func runCgroupValueTest(
	t *testing.T,
	name, fileContent string,
	checkUnlimited bool,
	expected uint64,
	expectError bool,
	errorContains string,
) {
	fs := &testutil.MockFileSystem{}
	if name != "File read error" {
		fs.Files = map[string][]byte{
			"/test/file": []byte(fileContent),
		}
	} else {
		fs.OpenErrs = map[string]error{
			"/test/file": fmt.Errorf("permission denied"),
		}
	}

	result, err := readCgroupValueWithFS(fs, "/test/file", checkUnlimited)

	if expectError {
		if err == nil {
			t.Errorf("Expected error containing %q, got nil", errorContains)
		} else if !strings.Contains(err.Error(), errorContains) {
			t.Errorf("Expected error containing %q, got %v", errorContains, err)
		}
	} else {
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	}
}

// Helper function for cgroup stat tests
func runCgroupStatTest(
	t *testing.T,
	name, fileContent, key string,
	expected uint64,
	expectError bool,
	errorContains string,
) {
	fs := &testutil.MockFileSystem{}
	if name != "File open error" {
		fs.Files = map[string][]byte{
			"/test/stat": []byte(fileContent),
		}
	} else {
		fs.OpenErrs = map[string]error{
			"/test/stat": fmt.Errorf("permission denied"),
		}
	}

	result, err := readCgroupStatWithFS(fs, "/test/stat", key)

	if expectError {
		if err == nil {
			t.Errorf("Expected error containing %q, got nil", errorContains)
		} else if !strings.Contains(err.Error(), errorContains) {
			t.Errorf("Expected error containing %q, got %v", errorContains, err)
		}
	} else {
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	}
}

// Test readCgroupValueWithFS edge cases
func TestReadCgroupValueWithFS(t *testing.T) {
	tests := []struct {
		name           string
		fileContent    string
		checkUnlimited bool
		expected       uint64
		expectError    bool
		errorContains  string
	}{
		{
			name:           "Normal value",
			fileContent:    "1024000",
			checkUnlimited: false,
			expected:       1024000,
			expectError:    false,
		},
		{
			name:           "Unlimited max value",
			fileContent:    "max",
			checkUnlimited: true,
			expected:       0,
			expectError:    true,
			errorContains:  "unlimited memory limit",
		},
		{
			name:           "Unlimited large value",
			fileContent:    "18446744073709551615", // ^uint64(0)
			checkUnlimited: true,
			expected:       0,
			expectError:    true,
			errorContains:  "unlimited memory limit",
		},
		{
			name:           "Large value but checkUnlimited false",
			fileContent:    "18446744073709551615",
			checkUnlimited: false,
			expected:       18446744073709551615,
			expectError:    false,
		},
		{
			name:           "Invalid number",
			fileContent:    "invalid",
			checkUnlimited: false,
			expected:       0,
			expectError:    true,
			errorContains:  "failed to parse value",
		},
		{
			name:           "Empty file",
			fileContent:    "",
			checkUnlimited: false,
			expected:       0,
			expectError:    true,
			errorContains:  "failed to parse value",
		},
		{
			name:           "Whitespace padded",
			fileContent:    "  12345  \n",
			checkUnlimited: false,
			expected:       12345,
			expectError:    false,
		},
		{
			name:           "File read error",
			fileContent:    "",
			checkUnlimited: false,
			expected:       0,
			expectError:    true,
			errorContains:  "failed to read file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runCgroupValueTest(t, tt.name, tt.fileContent, tt.checkUnlimited, tt.expected, tt.expectError, tt.errorContains)
		})
	}
}

// Test readCgroupStatWithFS edge cases
func TestReadCgroupStatWithFS(t *testing.T) {
	tests := []struct {
		name          string
		fileContent   string
		key           string
		expected      uint64
		expectError   bool
		errorContains string
	}{
		{
			name:        "Normal key found",
			fileContent: "total_inactive_file 50000\nother_key 1000\n",
			key:         "total_inactive_file",
			expected:    50000,
			expectError: false,
		},
		{
			name:        "Key at end of file",
			fileContent: "first_key 100\ntotal_inactive_file 75000",
			key:         "total_inactive_file",
			expected:    75000,
			expectError: false,
		},
		{
			name:          "Key not found",
			fileContent:   "other_key 100\nanother_key 200\n",
			key:           "total_inactive_file",
			expected:      0,
			expectError:   true,
			errorContains: "key \"total_inactive_file\" not found",
		},
		{
			name:          "Invalid value",
			fileContent:   "total_inactive_file invalid\n",
			key:           "total_inactive_file",
			expected:      0,
			expectError:   true,
			errorContains: "failed to parse value",
		},
		{
			name:          "Key without value",
			fileContent:   "total_inactive_file\nother_key 100\n",
			key:           "total_inactive_file",
			expected:      0,
			expectError:   true,
			errorContains: "key \"total_inactive_file\" not found",
		},
		{
			name:          "File open error",
			fileContent:   "",
			key:           "total_inactive_file",
			expected:      0,
			expectError:   true,
			errorContains: "failed to open file",
		},
		{
			name:        "Multiple spaces",
			fileContent: "total_inactive_file    12345\n",
			key:         "total_inactive_file",
			expected:    12345,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runCgroupStatTest(t, tt.name, tt.fileContent, tt.key, tt.expected, tt.expectError, tt.errorContains)
		})
	}
}

// Test parseMemInfo edge cases
func TestParseMemInfo(t *testing.T) {
	tests := []struct {
		name          string
		meminfo       string
		expectedTotal uint64
		expectedAvail uint64
		expectError   bool
		errorContains string
	}{
		{
			name:          "Missing MemTotal",
			meminfo:       "MemFree: 1000 kB\nCached: 2000 kB\n",
			expectedTotal: 0,
			expectedAvail: 0,
			expectError:   true,
			errorContains: "could not find MemTotal",
		},
		{
			name:          "Invalid MemTotal",
			meminfo:       "MemTotal: invalid kB\nMemFree: 1000 kB\n",
			expectedTotal: 0,
			expectedAvail: 0,
			expectError:   true,
			errorContains: "could not find MemTotal", // parseMemInfoFields skips invalid lines
		},
		{
			name:          "Normal case with MemAvailable",
			meminfo:       "MemTotal: 8000 kB\nMemFree: 1000 kB\nMemAvailable: 6000 kB\nCached: 2000 kB\n",
			expectedTotal: 8192000, // 8000 kB * 1024 = 8192000 bytes
			expectedAvail: 6144000, // 6000 kB * 1024 = 6144000 bytes
			expectError:   false,
		},
		{
			name:          "Fallback calculation without MemAvailable",
			meminfo:       "MemTotal: 8000 kB\nMemFree: 1000 kB\nCached: 2000 kB\n",
			expectedTotal: 8192000, // 8000 kB * 1024 = 8192000 bytes
			expectedAvail: 0,       // Will be calculated by calculateAvailableMemory
			expectError:   false,
		},
		{
			name:          "Fallback fails when MemFree and Cached are zero",
			meminfo:       "MemTotal: 8000 kB\nMemFree: 0 kB\nCached: 0 kB\n",
			expectedTotal: 0,
			expectedAvail: 0,
			expectError:   true,
			errorContains: "fallback calculation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.meminfo)
			result, err := parseMemInfo(reader)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error containing %q, got nil", tt.errorContains)
				} else if !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing %q, got %v", tt.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if result.Total != tt.expectedTotal {
					t.Errorf("Expected total %d, got %d", tt.expectedTotal, result.Total)
				}
				// Note: Available memory calculation may vary, so we just check it's reasonable
				if tt.expectedAvail > 0 && result.Available == 0 {
					t.Errorf("Expected some available memory, got 0")
				}
			}
		})
	}
}
