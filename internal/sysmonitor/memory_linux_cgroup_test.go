//go:build linux

package sysmonitor

import (
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
)

// mockFileSystem is a mock implementation of FileSystem for testing
type mockFileSystem struct {
	files map[string][]byte
	open  map[string]io.ReadCloser
}

func newMockFileSystem() *mockFileSystem {
	return &mockFileSystem{
		files: make(map[string][]byte),
		open:  make(map[string]io.ReadCloser),
	}
}

func (m *mockFileSystem) ReadFile(name string) ([]byte, error) {
	if data, ok := m.files[name]; ok {
		return data, nil
	}
	return nil, fs.ErrNotExist
}

func (m *mockFileSystem) Open(name string) (fs.File, error) {
	if file, ok := m.open[name]; ok {
		return &mockFile{reader: file}, nil
	}
	if data, ok := m.files[name]; ok {
		return &mockFile{reader: io.NopCloser(strings.NewReader(string(data)))}, nil
	}
	return nil, fs.ErrNotExist
}

// mockFile implements fs.File
type mockFile struct {
	reader io.ReadCloser
}

func (m *mockFile) Stat() (fs.FileInfo, error) {
	return nil, errors.New("not implemented")
}

func (m *mockFile) Read(p []byte) (int, error) {
	return m.reader.Read(p)
}

func (m *mockFile) Close() error {
	return m.reader.Close()
}

func TestReadCgroupValueWithFS(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		fileContent    string
		checkUnlimited bool
		want           uint64
		wantErr        bool
		errContains    string
	}{
		{
			name:           "valid value",
			path:           "/sys/fs/cgroup/memory.current",
			fileContent:    "1073741824",
			checkUnlimited: false,
			want:           1073741824,
			wantErr:        false,
		},
		{
			name:           "max value v2",
			path:           "/sys/fs/cgroup/memory.max",
			fileContent:    "max",
			checkUnlimited: false,
			want:           0,
			wantErr:        true,
			errContains:    "unlimited memory limit",
		},
		{
			name:           "unlimited v1 (large number)",
			path:           "/sys/fs/cgroup/memory/memory.limit_in_bytes",
			fileContent:    "9223372036854775808", // > 1<<60
			checkUnlimited: true,
			want:           0,
			wantErr:        true,
			errContains:    "unlimited memory limit",
		},
		{
			name:           "file not found",
			path:           "/nonexistent",
			fileContent:    "",
			checkUnlimited: false,
			want:           0,
			wantErr:        true,
			errContains:    "failed to read file",
		},
		{
			name:           "invalid number",
			path:           "/sys/fs/cgroup/memory.current",
			fileContent:    "not-a-number",
			checkUnlimited: false,
			want:           0,
			wantErr:        true,
			errContains:    "failed to parse value",
		},
		{
			name:           "zero value",
			path:           "/sys/fs/cgroup/memory.current",
			fileContent:    "0",
			checkUnlimited: false,
			want:           0,
			wantErr:        false,
		},
		{
			name:           "large valid value",
			path:           "/sys/fs/cgroup/memory.current",
			fileContent:    "8589934592", // 8GB
			checkUnlimited: false,
			want:           8589934592,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newMockFileSystem()
			if tt.fileContent != "" {
				fs.files[tt.path] = []byte(tt.fileContent)
			}

			got, err := readCgroupValueWithFS(fs, tt.path, tt.checkUnlimited)
			if (err != nil) != tt.wantErr {
				t.Errorf("readCgroupValueWithFS() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err != nil && tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("readCgroupValueWithFS() error = %v, want error containing %q", err, tt.errContains)
				}
			} else {
				if got != tt.want {
					t.Errorf("readCgroupValueWithFS() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestReadCgroupStatWithFS(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		key         string
		fileContent string
		want        uint64
		wantErr     bool
		errContains string
	}{
		{
			name:        "valid stat v2",
			path:        "/sys/fs/cgroup/memory.stat",
			key:         "inactive_file",
			fileContent: "inactive_file 1048576\nactive_file 2097152\n",
			want:        1048576,
			wantErr:     false,
		},
		{
			name:        "valid stat v1",
			path:        "/sys/fs/cgroup/memory/memory.stat",
			key:         "total_inactive_file",
			fileContent: "cache 4194304\ntotal_inactive_file 2097152\nrss 1048576\n",
			want:        2097152,
			wantErr:     false,
		},
		{
			name:        "key not found",
			path:        "/sys/fs/cgroup/memory.stat",
			key:         "nonexistent_key",
			fileContent: "inactive_file 1048576\nactive_file 2097152\n",
			want:        0,
			wantErr:     true,
			errContains: "not found",
		},
		{
			name:        "file not found",
			path:        "/nonexistent",
			key:         "inactive_file",
			fileContent: "",
			want:        0,
			wantErr:     true,
			errContains: "failed to open file",
		},
		{
			name:        "invalid value",
			path:        "/sys/fs/cgroup/memory.stat",
			key:         "inactive_file",
			fileContent: "inactive_file not-a-number\n",
			want:        0,
			wantErr:     true,
			errContains: "failed to parse value",
		},
		{
			name:        "key with no value",
			path:        "/sys/fs/cgroup/memory.stat",
			key:         "inactive_file",
			fileContent: "inactive_file\n",
			want:        0,
			wantErr:     true,
			errContains: "not found",
		},
		{
			name:        "key at end of line",
			path:        "/sys/fs/cgroup/memory.stat",
			key:         "inactive_file",
			fileContent: "inactive_file 1048576",
			want:        1048576,
			wantErr:     false,
		},
		{
			name:        "multiple spaces",
			path:        "/sys/fs/cgroup/memory.stat",
			key:         "inactive_file",
			fileContent: "inactive_file   1048576   \n",
			want:        1048576,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newMockFileSystem()
			if tt.fileContent != "" {
				fs.files[tt.path] = []byte(tt.fileContent)
			}

			got, err := readCgroupStatWithFS(fs, tt.path, tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("readCgroupStatWithFS() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err != nil && tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("readCgroupStatWithFS() error = %v, want error containing %q", err, tt.errContains)
				}
			} else {
				if got != tt.want {
					t.Errorf("readCgroupStatWithFS() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// cgroupMemoryTestCase represents a test case for readCgroupMemoryWithFS
type cgroupMemoryTestCase struct {
	name          string
	usage         string
	limit         string
	stat          string
	wantTotal     uint64
	wantAvailable uint64
	wantErr       bool
	errContains   string
}

// testCgroupMemoryCase is a helper function to test readCgroupMemoryWithFS with different file paths and configs
func testCgroupMemoryCase(
	t *testing.T,
	tt cgroupMemoryTestCase,
	usagePath, limitPath, statPath string,
	config cgroupMemoryConfig,
) {
	t.Helper()
	t.Run(tt.name, func(t *testing.T) {
		fs := newMockFileSystem()
		if tt.usage != "" {
			fs.files[usagePath] = []byte(tt.usage)
		}
		if tt.limit != "" {
			fs.files[limitPath] = []byte(tt.limit)
		}
		if tt.stat != "" {
			fs.files[statPath] = []byte(tt.stat)
		}

		got, err := readCgroupMemoryWithFS(fs, config)
		if (err != nil) != tt.wantErr {
			t.Errorf("readCgroupMemoryWithFS() error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		if tt.wantErr {
			if err != nil && tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("readCgroupMemoryWithFS() error = %v, want error containing %q", err, tt.errContains)
			}
		} else {
			if got.Total != tt.wantTotal {
				t.Errorf("readCgroupMemoryWithFS() Total = %v, want %v", got.Total, tt.wantTotal)
			}
			if got.Available != tt.wantAvailable {
				t.Errorf("readCgroupMemoryWithFS() Available = %v, want %v", got.Available, tt.wantAvailable)
			}
		}
	})
}

func TestReadCgroupMemoryWithFS_V2(t *testing.T) {
	tests := []cgroupMemoryTestCase{
		{
			name:          "normal case",
			usage:         "1073741824",                // 1GB
			limit:         "2147483648",                // 2GB
			stat:          "inactive_file 104857600\n", // 100MB
			wantTotal:     2147483648,
			wantAvailable: 1178599424, // (2GB - 1GB) + 100MB = 1073741824 + 104857600
			wantErr:       false,
		},
		{
			name:          "usage exceeds limit",
			usage:         "2147483648",                // 2GB
			limit:         "1073741824",                // 1GB
			stat:          "inactive_file 104857600\n", // 100MB
			wantTotal:     1073741824,
			wantAvailable: 104857600, // Only reclaimable memory
			wantErr:       false,
		},
		{
			name:          "no inactive_file stat",
			usage:         "1073741824",
			limit:         "2147483648",
			stat:          "active_file 104857600\n",
			wantTotal:     2147483648,
			wantAvailable: 1073741824, // (2GB - 1GB) + 0
			wantErr:       false,
		},
		{
			name:          "available exceeds limit",
			usage:         "1073741824",
			limit:         "2147483648",
			stat:          "inactive_file 2147483648\n", // 2GB (would make available > limit)
			wantTotal:     2147483648,
			wantAvailable: 2147483648, // Capped at limit
			wantErr:       false,
		},
		{
			name:          "zero usage",
			usage:         "0",
			limit:         "2147483648",
			stat:          "inactive_file 104857600\n",
			wantTotal:     2147483648,
			wantAvailable: 2147483648, // 2GB + 100MB = 2252341248, but capped at limit (2GB)
			wantErr:       false,
		},
		{
			name:          "missing usage file",
			usage:         "",
			limit:         "2147483648",
			stat:          "inactive_file 104857600\n",
			wantTotal:     0,
			wantAvailable: 0,
			wantErr:       true,
			errContains:   "failed to read cgroup v2 memory usage",
		},
		{
			name:          "missing limit file",
			usage:         "1073741824",
			limit:         "",
			stat:          "inactive_file 104857600\n",
			wantTotal:     0,
			wantAvailable: 0,
			wantErr:       true,
			errContains:   "failed to read cgroup v2 memory limit",
		},
		{
			name:          "unlimited limit",
			usage:         "1073741824",
			limit:         "max",
			stat:          "inactive_file 104857600\n",
			wantTotal:     0,
			wantAvailable: 0,
			wantErr:       true,
			errContains:   "unlimited memory limit",
		},
	}

	for _, tt := range tests {
		testCgroupMemoryCase(t, tt,
			"/sys/fs/cgroup/memory.current",
			"/sys/fs/cgroup/memory.max",
			"/sys/fs/cgroup/memory.stat",
			cgroupV2Config)
	}
}

func TestReadCgroupMemoryWithFS_V1(t *testing.T) {
	tests := []cgroupMemoryTestCase{
		{
			name:          "normal case",
			usage:         "1073741824",                      // 1GB
			limit:         "2147483648",                      // 2GB
			stat:          "total_inactive_file 104857600\n", // 100MB
			wantTotal:     2147483648,
			wantAvailable: 1178599424, // (2GB - 1GB) + 100MB = 1073741824 + 104857600
			wantErr:       false,
		},
		{
			name:          "usage exceeds limit",
			usage:         "2147483648",                      // 2GB
			limit:         "1073741824",                      // 1GB
			stat:          "total_inactive_file 104857600\n", // 100MB
			wantTotal:     1073741824,
			wantAvailable: 104857600, // Only reclaimable memory
			wantErr:       false,
		},
		{
			name:          "unlimited limit (large number)",
			usage:         "1073741824",
			limit:         "9223372036854775808", // > 1<<60
			stat:          "total_inactive_file 104857600\n",
			wantTotal:     0,
			wantAvailable: 0,
			wantErr:       true,
			errContains:   "unlimited memory limit",
		},
		{
			name:          "missing stat file (should default to 0)",
			usage:         "1073741824",
			limit:         "2147483648",
			stat:          "",
			wantTotal:     2147483648,
			wantAvailable: 1073741824, // (2GB - 1GB) + 0
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		testCgroupMemoryCase(t, tt,
			"/sys/fs/cgroup/memory/memory.usage_in_bytes",
			"/sys/fs/cgroup/memory/memory.limit_in_bytes",
			"/sys/fs/cgroup/memory/memory.stat",
			cgroupV1Config)
	}
}

func TestMakeCgroupV2Reader(t *testing.T) {
	fs := newMockFileSystem()
	fs.files["/sys/fs/cgroup/memory.current"] = []byte("1073741824")
	fs.files["/sys/fs/cgroup/memory.max"] = []byte("2147483648")
	fs.files["/sys/fs/cgroup/memory.stat"] = []byte("inactive_file 104857600\n")

	reader := makeCgroupV2Reader(fs)
	mem, err := reader()
	if err != nil {
		t.Fatalf("makeCgroupV2Reader() error = %v", err)
	}

	if mem.Total != 2147483648 {
		t.Errorf("makeCgroupV2Reader() Total = %v, want %v", mem.Total, 2147483648)
	}
	if mem.Available != 1178599424 {
		t.Errorf("makeCgroupV2Reader() Available = %v, want %v", mem.Available, 1178599424)
	}
}

func TestMakeCgroupV1Reader(t *testing.T) {
	fs := newMockFileSystem()
	fs.files["/sys/fs/cgroup/memory/memory.usage_in_bytes"] = []byte("1073741824")
	fs.files["/sys/fs/cgroup/memory/memory.limit_in_bytes"] = []byte("2147483648")
	fs.files["/sys/fs/cgroup/memory/memory.stat"] = []byte("total_inactive_file 104857600\n")

	reader := makeCgroupV1Reader(fs)
	mem, err := reader()
	if err != nil {
		t.Fatalf("makeCgroupV1Reader() error = %v", err)
	}

	if mem.Total != 2147483648 {
		t.Errorf("makeCgroupV1Reader() Total = %v, want %v", mem.Total, 2147483648)
	}
	if mem.Available != 1178599424 {
		t.Errorf("makeCgroupV1Reader() Available = %v, want %v", mem.Available, 1178599424)
	}
}

func TestReadCgroupMemoryWithFS_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		config      cgroupMemoryConfig
		setupFS     func(*mockFileSystem)
		wantErr     bool
		errContains string
		validate    func(*testing.T, SystemMemory)
	}{
		{
			name:   "exact limit equals usage",
			config: cgroupV2Config,
			setupFS: func(fs *mockFileSystem) {
				fs.files["/sys/fs/cgroup/memory.current"] = []byte("1073741824")
				fs.files["/sys/fs/cgroup/memory.max"] = []byte("1073741824")
				fs.files["/sys/fs/cgroup/memory.stat"] = []byte("inactive_file 104857600\n")
			},
			wantErr: false,
			validate: func(t *testing.T, mem SystemMemory) {
				if mem.Total != 1073741824 {
					t.Errorf("Total = %v, want %v", mem.Total, 1073741824)
				}
				if mem.Available != 104857600 {
					t.Errorf("Available = %v, want %v", mem.Available, 104857600)
				}
			},
		},
		{
			name:   "very large values",
			config: cgroupV2Config,
			setupFS: func(fs *mockFileSystem) {
				fs.files["/sys/fs/cgroup/memory.current"] = []byte("17179869184")             // 16GB
				fs.files["/sys/fs/cgroup/memory.max"] = []byte("34359738368")                 // 32GB
				fs.files["/sys/fs/cgroup/memory.stat"] = []byte("inactive_file 1073741824\n") // 1GB
			},
			wantErr: false,
			validate: func(t *testing.T, mem SystemMemory) {
				if mem.Total != 34359738368 {
					t.Errorf("Total = %v, want %v", mem.Total, 34359738368)
				}
				if mem.Available != 18253611008 { // (32GB - 16GB) + 1GB
					t.Errorf("Available = %v, want %v", mem.Available, 18253611008)
				}
			},
		},
		{
			name:   "stat file read error (should default to 0)",
			config: cgroupV2Config,
			setupFS: func(fs *mockFileSystem) {
				fs.files["/sys/fs/cgroup/memory.current"] = []byte("1073741824")
				fs.files["/sys/fs/cgroup/memory.max"] = []byte("2147483648")
				// stat file not set, should default to 0
			},
			wantErr: false,
			validate: func(t *testing.T, mem SystemMemory) {
				if mem.Total != 2147483648 {
					t.Errorf("Total = %v, want %v", mem.Total, 2147483648)
				}
				if mem.Available != 1073741824 { // (2GB - 1GB) + 0
					t.Errorf("Available = %v, want %v", mem.Available, 1073741824)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newMockFileSystem()
			tt.setupFS(fs)

			got, err := readCgroupMemoryWithFS(fs, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("readCgroupMemoryWithFS() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err != nil && tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("readCgroupMemoryWithFS() error = %v, want error containing %q", err, tt.errContains)
				}
			} else {
				tt.validate(t, got)
			}
		})
	}
}
