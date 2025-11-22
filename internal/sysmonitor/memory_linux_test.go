//go:build linux

package sysmonitor

import (
	"errors"
	"io/fs"
	"strings"
	"testing"
)

// ===== Mock filesystem implementation =====

type mockFS map[string]string

func (m mockFS) ReadFile(name string) ([]byte, error) {
	if content, ok := m[name]; ok {
		return []byte(content), nil
	}
	return nil, errors.New("file does not exist: " + name)
}

func (m mockFS) Open(name string) (fs.File, error) {
	if content, ok := m[name]; ok {
		return &mockFile{content: content}, nil
	}
	return nil, errors.New("file does not exist: " + name)
}

type mockFile struct {
	content string
	reader  *strings.Reader
}

func (m *mockFile) Read(p []byte) (int, error) {
	if m.reader == nil {
		m.reader = strings.NewReader(m.content)
	}
	return m.reader.Read(p)
}

func (m *mockFile) Close() error { return nil }

func (m *mockFile) Stat() (fs.FileInfo, error) { return nil, nil }

// ===== Test functions =====

func TestReadCgroupV2Memory(t *testing.T) {
	mockFS := mockFS{
		"/sys/fs/cgroup/memory.current": "1073741824\n",              // 1GB
		"/sys/fs/cgroup/memory.max":     "2147483648\n",              // 2GB
		"/sys/fs/cgroup/memory.stat":    "inactive_file 104857600\n", // 100MB
	}

	mem, err := readCgroupMemoryWithFS(mockFS, cgroupV2Config)
	if err != nil {
		t.Fatalf("readCgroupMemoryWithFS failed: %v", err)
	}

	expectedTotal := uint64(2147483648)     // 2GB
	expectedAvailable := uint64(1178599424) // (2GB - 1GB) + 100MB = 1.1GB available

	if mem.Total != expectedTotal {
		t.Errorf("Expected total %d, got %d", expectedTotal, mem.Total)
	}

	if mem.Available != expectedAvailable {
		t.Errorf("Expected available %d, got %d", expectedAvailable, mem.Available)
	}
}

func TestReadCgroupV1Memory(t *testing.T) {
	mockFS := mockFS{
		"/sys/fs/cgroup/memory/memory.usage_in_bytes": "1073741824\n",                    // 1GB
		"/sys/fs/cgroup/memory/memory.limit_in_bytes": "2147483648\n",                    // 2GB
		"/sys/fs/cgroup/memory/memory.stat":           "total_inactive_file 104857600\n", // 100MB
	}

	mem, err := readCgroupMemoryWithFS(mockFS, cgroupV1Config)
	if err != nil {
		t.Fatalf("readCgroupMemoryWithFS failed: %v", err)
	}

	expectedTotal := uint64(2147483648)     // 2GB
	expectedAvailable := uint64(1178599424) // (2GB - 1GB) + 100MB = 1.1GB available

	if mem.Total != expectedTotal {
		t.Errorf("Expected total %d, got %d", expectedTotal, mem.Total)
	}

	if mem.Available != expectedAvailable {
		t.Errorf("Expected available %d, got %d", expectedAvailable, mem.Available)
	}
}

func TestReadCgroupValueWithFS(t *testing.T) {
	mockFS := mockFS{
		"/test/file": "123456\n",
	}

	value, err := readCgroupValueWithFS(mockFS, "/test/file")
	if err != nil {
		t.Fatalf("readCgroupValueWithFS failed: %v", err)
	}

	expected := uint64(123456)
	if value != expected {
		t.Errorf("Expected %d, got %d", expected, value)
	}

	// Test "max" value (unlimited)
	mockFS["/test/max"] = "max\n"
	_, err = readCgroupValueWithFS(mockFS, "/test/max")
	if err == nil {
		t.Error("Expected error for 'max' value")
	}
}

func TestReadCgroupStatWithFS(t *testing.T) {
	mockFS := mockFS{
		"/test/stat": "inactive_file 987654\nactive_file 123456\n",
	}

	value, err := readCgroupStatWithFS(mockFS, "/test/stat", "inactive_file")
	if err != nil {
		t.Fatalf("readCgroupStatWithFS failed: %v", err)
	}

	expected := uint64(987654)
	if value != expected {
		t.Errorf("Expected %d, got %d", expected, value)
	}

	// Test key not found
	_, err = readCgroupStatWithFS(mockFS, "/test/stat", "nonexistent_key")
	if err == nil {
		t.Error("Expected error for nonexistent key")
	}
}

func TestReadCgroupV2Memory_UsageExceedsLimit(t *testing.T) {
	mockFS := mockFS{
		"/sys/fs/cgroup/memory.current": "3000000000\n",              // 3GB (exceeds limit)
		"/sys/fs/cgroup/memory.max":     "2147483648\n",              // 2GB
		"/sys/fs/cgroup/memory.stat":    "inactive_file 104857600\n", // 100MB
	}

	mem, err := readCgroupMemoryWithFS(mockFS, cgroupV2Config)
	if err != nil {
		t.Fatalf("readCgroupMemoryWithFS failed: %v", err)
	}

	expectedTotal := uint64(2147483648)    // 2GB
	expectedAvailable := uint64(104857600) // Only reclaimable memory available when usage > limit

	if mem.Total != expectedTotal {
		t.Errorf("Expected total %d, got %d", expectedTotal, mem.Total)
	}

	if mem.Available != expectedAvailable {
		t.Errorf("Expected available %d, got %d", expectedAvailable, mem.Available)
	}
}

func TestReadCgroupV2Memory_Unlimited(t *testing.T) {
	mockFS := mockFS{
		"/sys/fs/cgroup/memory.current": "1073741824\n",              // 1GB
		"/sys/fs/cgroup/memory.max":     "max\n",                     // Unlimited
		"/sys/fs/cgroup/memory.stat":    "inactive_file 104857600\n", // 100MB
	}

	_, err := readCgroupMemoryWithFS(mockFS, cgroupV2Config)
	if err == nil {
		t.Error("Expected error for unlimited memory (max)")
	}
}

func TestReadCgroupV2Memory_MissingStatFile(t *testing.T) {
	mockFS := mockFS{
		"/sys/fs/cgroup/memory.current": "1073741824\n", // 1GB
		"/sys/fs/cgroup/memory.max":     "2147483648\n", // 2GB
		// memory.stat is missing
	}

	mem, err := readCgroupMemoryWithFS(mockFS, cgroupV2Config)
	if err != nil {
		t.Fatalf("readCgroupMemoryWithFS failed: %v", err)
	}

	expectedTotal := uint64(2147483648)     // 2GB
	expectedAvailable := uint64(1073741824) // (2GB - 1GB) + 0 = 1GB (no reclaimable)

	if mem.Total != expectedTotal {
		t.Errorf("Expected total %d, got %d", expectedTotal, mem.Total)
	}

	if mem.Available != expectedAvailable {
		t.Errorf("Expected available %d, got %d", expectedAvailable, mem.Available)
	}
}

func TestReadCgroupV1Memory_Unlimited(t *testing.T) {
	mockFS := mockFS{
		"/sys/fs/cgroup/memory/memory.usage_in_bytes": "1073741824\n",                    // 1GB
		"/sys/fs/cgroup/memory/memory.limit_in_bytes": "18446744073709551615\n",          // Huge number (unlimited)
		"/sys/fs/cgroup/memory/memory.stat":           "total_inactive_file 104857600\n", // 100MB
	}

	_, err := readCgroupMemoryWithFS(mockFS, cgroupV1Config)
	if err == nil {
		t.Error("Expected error for unlimited memory")
	}
}
