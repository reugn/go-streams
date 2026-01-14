package testutil

import (
	"errors"
	"io"
	"io/fs"
)

// MockFileSystem implements sysmonitor.FileSystem for testing.
// It provides an in-memory file system for testing file operations.
type MockFileSystem struct {
	Files    map[string][]byte
	OpenErrs map[string]error
}

func (m *MockFileSystem) ReadFile(name string) ([]byte, error) {
	if content, ok := m.Files[name]; ok {
		return content, nil
	}
	return nil, errors.New("file not found")
}

func (m *MockFileSystem) Open(name string) (fs.File, error) {
	if err, ok := m.OpenErrs[name]; ok {
		return nil, err
	}
	if content, ok := m.Files[name]; ok {
		return &mockFile{content: content, pos: 0}, nil
	}
	return nil, errors.New("file not found")
}

// mockFile implements fs.File for testing.
type mockFile struct {
	content []byte
	pos     int
}

func (f *mockFile) Read(p []byte) (n int, err error) {
	if f.pos >= len(f.content) {
		return 0, io.EOF
	}
	n = copy(p, f.content[f.pos:])
	f.pos += n
	return n, nil
}

func (f *mockFile) Close() error {
	return nil
}

func (f *mockFile) Stat() (fs.FileInfo, error) {
	return nil, errors.New("Stat not implemented")
}
