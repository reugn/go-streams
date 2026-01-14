package sysmonitor

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// MockFS is a mock implementation of FileSystem for testing
type MockFS struct {
	ReadFileFunc func(name string) ([]byte, error)
	OpenFunc     func(name string) (fs.File, error)
}

func (m MockFS) ReadFile(name string) ([]byte, error) {
	if m.ReadFileFunc != nil {
		return m.ReadFileFunc(name)
	}
	return nil, nil
}

func (m MockFS) Open(name string) (fs.File, error) {
	if m.OpenFunc != nil {
		return m.OpenFunc(name)
	}
	return nil, nil
}

func TestOSFileSystem_ReadFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Test successful read
	testContent := "Hello, World!"
	testFile := filepath.Join(tempDir, "test.txt")
	err := os.WriteFile(testFile, []byte(testContent), 0o600)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fs := OSFileSystem{}
	data, err := fs.ReadFile(testFile)
	if err != nil {
		t.Errorf("ReadFile failed: %v", err)
	}
	if string(data) != testContent {
		t.Errorf("Expected %q, got %q", testContent, string(data))
	}

	// Test reading non-existent file
	_, err = fs.ReadFile(filepath.Join(tempDir, "nonexistent.txt"))
	if err == nil {
		t.Error("Expected error when reading non-existent file, got nil")
	}
	if !os.IsNotExist(err) {
		t.Errorf("Expected IsNotExist error, got %v", err)
	}
}

func TestOSFileSystem_Open(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Test successful open
	testContent := "Hello, World!"
	testFile := filepath.Join(tempDir, "test.txt")
	err := os.WriteFile(testFile, []byte(testContent), 0o600)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fs := OSFileSystem{}
	file, err := fs.Open(testFile)
	if err != nil {
		t.Errorf("Open failed: %v", err)
	}
	defer file.Close()

	// Verify we can read from the opened file
	data := make([]byte, len(testContent))
	n, err := file.Read(data)
	if err != nil {
		t.Errorf("Failed to read from opened file: %v", err)
	}
	if n != len(testContent) {
		t.Errorf("Expected to read %d bytes, got %d", len(testContent), n)
	}
	if string(data) != testContent {
		t.Errorf("Expected %q, got %q", testContent, string(data))
	}

	// Test opening non-existent file
	_, err = fs.Open(filepath.Join(tempDir, "nonexistent.txt"))
	if err == nil {
		t.Error("Expected error when opening non-existent file, got nil")
	}
	if !os.IsNotExist(err) {
		t.Errorf("Expected IsNotExist error, got %v", err)
	}
}

func TestMockFS_ReadFile(t *testing.T) {
	expectedData := []byte("mock data")
	expectedErr := os.ErrNotExist

	mock := MockFS{
		ReadFileFunc: func(name string) ([]byte, error) {
			if name == "success.txt" {
				return expectedData, nil
			}
			return nil, expectedErr
		},
	}

	// Test success case
	data, err := mock.ReadFile("success.txt")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if string(data) != string(expectedData) {
		t.Errorf("Expected %q, got %q", expectedData, data)
	}

	// Test error case
	_, err = mock.ReadFile("error.txt")
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}

	// Test default behavior (no function set)
	mockDefault := MockFS{}
	data, err = mockDefault.ReadFile("any.txt")
	if err != nil {
		t.Errorf("Expected no error for default mock, got %v", err)
	}
	if data != nil {
		t.Errorf("Expected nil data for default mock, got %v", data)
	}
}

func TestMockFS_Open(t *testing.T) {
	expectedErr := os.ErrNotExist

	mock := MockFS{
		OpenFunc: func(name string) (fs.File, error) {
			if name == "error.txt" {
				return nil, expectedErr
			}
			return nil, nil // Mock file (nil for simplicity in tests)
		},
	}

	// Test error case
	_, err := mock.Open("error.txt")
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}

	// Test success case (returns nil file for simplicity)
	file, err := mock.Open("success.txt")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if file != nil {
		t.Errorf("Expected nil file for mock, got %v", file)
	}

	// Test default behavior (no function set)
	mockDefault := MockFS{}
	file, err = mockDefault.Open("any.txt")
	if err != nil {
		t.Errorf("Expected no error for default mock, got %v", err)
	}
	if file != nil {
		t.Errorf("Expected nil file for default mock, got %v", file)
	}
}

func TestFileSystemInterface(_ *testing.T) {
	// Test that OSFileSystem implements FileSystem interface
	var _ FileSystem = OSFileSystem{}

	// Test that MockFS implements FileSystem interface
	var _ FileSystem = MockFS{}
}
