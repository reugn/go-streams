package sysmonitor

import (
	"io/fs"
	"os"
)

// FileSystem abstracts file system operations for testing
type FileSystem interface {
	ReadFile(name string) ([]byte, error)
	Open(name string) (fs.File, error)
}

// OSFileSystem implements FileSystem using the os package
type OSFileSystem struct{}

func (OSFileSystem) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func (OSFileSystem) Open(name string) (fs.File, error) {
	return os.Open(name)
}
