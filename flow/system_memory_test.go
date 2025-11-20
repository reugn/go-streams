package flow

import (
	"errors"
	"io/fs"
	"strings"
	"testing"
)

func TestReadSystemMemory(t *testing.T) {
	mem, err := readSystemMemory()
	if err != nil {
		t.Fatalf("readSystemMemory failed: %v", err)
	}

	if mem.Total == 0 {
		t.Error("Total memory should not be zero")
	}

	if mem.Available > mem.Total {
		t.Errorf("Available memory (%d) should not exceed total memory (%d)", mem.Available, mem.Total)
	}

	// Available should be less than or equal to total
	if mem.Available > mem.Total {
		t.Errorf("Available memory (%d) > total memory (%d)", mem.Available, mem.Total)
	}
}

func TestReadSystemMemoryAuto(t *testing.T) {
	// Save original state to prevent global state pollution
	original := loadSystemMemoryReader()
	defer systemMemoryProvider.Store(original)

	mem1, err1 := readSystemMemoryAuto()
	if err1 != nil {
		t.Fatalf("First readSystemMemoryAuto failed: %v", err1)
	}

	// After the first call, the global reader should have been upgraded
	// Call the upgraded reader directly to ensure consistency
	upgradedReader := loadSystemMemoryReader()
	mem2, err2 := upgradedReader()
	if err2 != nil {
		t.Fatalf("Upgraded reader failed: %v", err2)
	}

	// Results should be consistent (within reasonable bounds due to memory fluctuations)
	if mem1.Total != mem2.Total {
		t.Errorf("Total memory inconsistent: first=%d, second=%d",
			mem1.Total, mem2.Total)
	}

	// Allow for small variations in available memory (due to system activity)
	const tolerance uint64 = 1024 * 1024 // 1MB tolerance
	if mem1.Available > mem2.Available {
		if mem1.Available-mem2.Available > tolerance {
			t.Errorf("Available memory varies too much: first=%d, second=%d (tolerance: %d)",
				mem1.Available, mem2.Available, tolerance)
		}
	} else {
		if mem2.Available-mem1.Available > tolerance {
			t.Errorf("Available memory varies too much: first=%d, second=%d (tolerance: %d)",
				mem1.Available, mem2.Available, tolerance)
		}
	}
}

func TestGetSystemMemory(t *testing.T) {
	mem, err := getSystemMemory()
	if err != nil {
		t.Fatalf("getSystemMemory failed: %v", err)
	}

	if mem.Total == 0 {
		t.Error("Total memory should not be zero")
	}

	if mem.Available > mem.Total {
		t.Errorf("Available memory (%d) should not exceed total memory (%d)", mem.Available, mem.Total)
	}
}

func TestSetSystemMemoryReader(t *testing.T) {
	// Create a mock reader
	mockReader := func() (systemMemory, error) {
		return systemMemory{
			Total:     1000000,
			Available: 500000,
		}, nil
	}

	// Set the mock reader
	restore := setSystemMemoryReader(mockReader)

	// Test that getSystemMemory uses the mock
	mem, err := getSystemMemory()
	if err != nil {
		t.Fatalf("getSystemMemory with mock failed: %v", err)
	}

	if mem.Total != 1000000 || mem.Available != 500000 {
		t.Errorf("Mock reader not used: expected (1000000, 500000), got (%d, %d)", mem.Total, mem.Available)
	}

	// Restore and test that original reader is back
	restore()
	mem2, err := getSystemMemory()
	if err != nil {
		t.Fatalf("getSystemMemory after restore failed: %v", err)
	}

	// Should not be using the mock anymore
	if mem2.Total == 1000000 && mem2.Available == 500000 {
		t.Error("Original reader not restored")
	}
}

func TestSystemMemoryReaderErrorHandling(t *testing.T) {
	// Test with a reader that returns an error
	errorReader := func() (systemMemory, error) {
		return systemMemory{}, errors.New("mock error")
	}

	restore := setSystemMemoryReader(errorReader)
	defer restore()

	_, err := getSystemMemory()
	if err == nil {
		t.Error("Expected error from mock reader")
	}
}

// Test concurrent access to system memory reader
func TestConcurrentSystemMemoryAccess(t *testing.T) {
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_, err := getSystemMemory()
				if err != nil {
					t.Errorf("Concurrent access failed: %v", err)
				}
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

// Test memory calculations are reasonable
func TestMemoryCalculations(t *testing.T) {
	mem, err := getSystemMemory()
	if err != nil {
		t.Fatalf("Failed to get system memory: %v", err)
	}

	// Available should be less than or equal to total
	if mem.Available > mem.Total {
		t.Errorf("Available memory (%d) exceeds total memory (%d)", mem.Available, mem.Total)
	}

	// Used memory calculation
	used := mem.Total - mem.Available
	usedPercent := float64(used) / float64(mem.Total) * 100.0

	if usedPercent < 0 || usedPercent > 100 {
		t.Errorf("Used percentage should be between 0-100, got %.2f%%", usedPercent)
	}

	t.Logf("Memory usage: Total=%dMB, Used=%dMB (%.1f%%), Available=%dMB",
		mem.Total/1024/1024,
		used/1024/1024,
		usedPercent,
		mem.Available/1024/1024)
}

// mockFileSystem implements FileSystem for testing
type mockFileSystem struct {
	files map[string]string
}

func (m *mockFileSystem) ReadFile(name string) ([]byte, error) {
	if content, ok := m.files[name]; ok {
		return []byte(content), nil
	}
	return nil, fs.ErrNotExist
}

func (m *mockFileSystem) Open(name string) (fs.File, error) {
	if content, ok := m.files[name]; ok {
		return &mockFile{content: content}, nil
	}
	return nil, fs.ErrNotExist
}

// mockFile implements fs.File for testing
type mockFile struct {
	content string
	reader  *strings.Reader
}

func (m *mockFile) Read(b []byte) (int, error) {
	if m.reader == nil {
		m.reader = strings.NewReader(m.content)
	}
	return m.reader.Read(b)
}

func (m *mockFile) Close() error {
	return nil
}

func (m *mockFile) Stat() (fs.FileInfo, error) {
	return nil, fs.ErrNotExist
}

// cgroupTestCase represents a test case for cgroup memory reading
type cgroupTestCase struct {
	name        string
	usage       string
	limit       string
	stat        string
	expectTotal uint64
	expectAvail uint64
	expectError bool
}

// runCgroupTest runs a generic test for cgroup memory reading functions
func runCgroupTest(t *testing.T, testCases []cgroupTestCase,
	readFunc func(FileSystem) (systemMemory, error), usagePath, limitPath, statPath string) {
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			mockFS := &mockFileSystem{
				files: map[string]string{
					usagePath: tt.usage,
					limitPath: tt.limit,
					statPath:  tt.stat,
				},
			}

			mem, err := readFunc(mockFS)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if mem.Total != tt.expectTotal {
				t.Errorf("expected total %d, got %d", tt.expectTotal, mem.Total)
			}

			if mem.Available != tt.expectAvail {
				t.Errorf("expected available %d, got %d", tt.expectAvail, mem.Available)
			}
		})
	}
}

func TestReadCgroupV2Memory(t *testing.T) {
	testCases := []cgroupTestCase{
		{
			name:        "normal cgroup v2",
			usage:       "104857600",              // 100MB
			limit:       "209715200",              // 200MB
			stat:        "inactive_file 52428800", // 50MB
			expectTotal: 209715200,                // 200MB
			expectAvail: 157286400,                // 150MB (200-100+50)
			expectError: false,
		},
		{
			name:        "unlimited memory",
			usage:       "104857600", // 100MB
			limit:       "max",
			stat:        "inactive_file 0",
			expectError: true, // max should cause error
		},
		{
			name:        "available exceeds limit",
			usage:       "104857600",               // 100MB
			limit:       "209715200",               // 200MB
			stat:        "inactive_file 209715200", // 200MB (would make available = 300MB)
			expectTotal: 209715200,                 // 200MB
			expectAvail: 209715200,                 // capped at limit = 200MB
			expectError: false,
		},
		{
			name:        "usage exceeds limit (underflow check)",
			usage:       "209715201", // 200MB + 1 byte
			limit:       "209715200", // 200MB
			stat:        "inactive_file 0",
			expectTotal: 209715200, // 200MB
			expectAvail: 0,         // Should be 0, not wrapped uint64
			expectError: false,
		},
	}

	runCgroupTest(t, testCases, readCgroupV2MemoryWithFS,
		"/sys/fs/cgroup/memory.current",
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory.stat")
}

func TestReadCgroupV1Memory(t *testing.T) {
	testCases := []cgroupTestCase{
		{
			name:        "normal cgroup v1",
			usage:       "104857600",                    // 100MB
			limit:       "209715200",                    // 200MB
			stat:        "total_inactive_file 52428800", // 50MB
			expectTotal: 209715200,                      // 200MB
			expectAvail: 157286400,                      // 150MB (200-100+50)
			expectError: false,
		},
		{
			name:        "unlimited memory",
			usage:       "104857600",            // 100MB
			limit:       "18446744073709551615", // unlimited (2^64-1)
			stat:        "total_inactive_file 0",
			expectError: true, // unlimited should cause error
		},
		{
			name:        "available exceeds limit",
			usage:       "104857600",                     // 100MB
			limit:       "209715200",                     // 200MB
			stat:        "total_inactive_file 209715200", // 200MB (would make available = 300MB)
			expectTotal: 209715200,                       // 200MB
			expectAvail: 209715200,                       // capped at limit = 200MB
			expectError: false,
		},
		{
			name:        "usage exceeds limit (underflow check)",
			usage:       "209715201", // 200MB + 1 byte
			limit:       "209715200", // 200MB
			stat:        "total_inactive_file 0",
			expectTotal: 209715200, // 200MB
			expectAvail: 0,         // Should be 0, not wrapped uint64
			expectError: false,
		},
	}

	runCgroupTest(t, testCases, readCgroupV1MemoryWithFS,
		"/sys/fs/cgroup/memory/memory.usage_in_bytes",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
		"/sys/fs/cgroup/memory/memory.stat")
}

func TestReadCgroupValueWithFS(t *testing.T) {
	mockFS := &mockFileSystem{
		files: map[string]string{
			"/test/file": "123456789",
			"/test/max":  "max",
		},
	}

	// Test normal value
	val, err := readCgroupValueWithFS(mockFS, "/test/file")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 123456789 {
		t.Errorf("expected 123456789, got %d", val)
	}

	// Test "max" value
	_, err = readCgroupValueWithFS(mockFS, "/test/max")
	if err == nil {
		t.Error("expected error for 'max' value")
	}
}

func TestReadCgroupStatWithFS(t *testing.T) {
	mockFS := &mockFileSystem{
		files: map[string]string{
			"/test/stat": "inactive_file 52428800\ntotal_cache 104857600\n",
		},
	}

	// Test existing key
	val, err := readCgroupStatWithFS(mockFS, "/test/stat", "inactive_file")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 52428800 {
		t.Errorf("expected 52428800, got %d", val)
	}

	// Test non-existing key
	_, err = readCgroupStatWithFS(mockFS, "/test/stat", "nonexistent")
	if err == nil {
		t.Error("expected error for non-existent key")
	}
}
