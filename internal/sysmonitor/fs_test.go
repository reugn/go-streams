package sysmonitor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOSFileSystem_ReadFile(t *testing.T) {
	fs := OSFileSystem{}

	t.Run("read existing file", func(t *testing.T) {
		// Create a temporary file
		tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.txt")
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		testContent := []byte("test content")
		if _, err := tmpFile.Write(testContent); err != nil {
			t.Fatalf("failed to write to temp file: %v", err)
		}
		tmpFile.Close()

		// Read the file
		content, err := fs.ReadFile(tmpFile.Name())
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}

		if string(content) != string(testContent) {
			t.Errorf("expected content %q, got %q", string(testContent), string(content))
		}
	})

	t.Run("read non-existent file", func(t *testing.T) {
		nonExistentFile := filepath.Join(os.TempDir(), "non-existent-file-12345.txt")
		_, err := fs.ReadFile(nonExistentFile)
		if err == nil {
			t.Error("expected error when reading non-existent file")
		}
		if !os.IsNotExist(err) {
			t.Errorf("expected os.IsNotExist error, got %v", err)
		}
	})
}

func TestOSFileSystem_Open(t *testing.T) {
	fs := OSFileSystem{}

	t.Run("open existing file", func(t *testing.T) {
		// Create a temporary file
		tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.txt")
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		testContent := []byte("test content")
		if _, err := tmpFile.Write(testContent); err != nil {
			t.Fatalf("failed to write to temp file: %v", err)
		}
		tmpFile.Close()

		// Open the file
		file, err := fs.Open(tmpFile.Name())
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer file.Close()

		// Verify we can read from it
		stat, err := file.Stat()
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if stat.Size() != int64(len(testContent)) {
			t.Errorf("expected file size %d, got %d", len(testContent), stat.Size())
		}
	})

	t.Run("open non-existent file", func(t *testing.T) {
		nonExistentFile := filepath.Join(os.TempDir(), "non-existent-file-12345.txt")
		_, err := fs.Open(nonExistentFile)
		if err == nil {
			t.Error("expected error when opening non-existent file")
		}
		if !os.IsNotExist(err) {
			t.Errorf("expected os.IsNotExist error, got %v", err)
		}
	})
}
