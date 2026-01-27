package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type WAL struct {
	file *os.File
	mu   sync.Mutex // Ensure only one operation at a time
}

func NewWAL(dataDir string, topic string) (*WAL, error) {
	path := filepath.Join(dataDir, topic)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	filename := filepath.Join(path, "0000.log")
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

// Format on Disk: [Length (4 bytes)] + [Data]
func (w *WAL) Write(data []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. get current file size (this acts as our "ID" or "Offset")
	stat, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	offset := stat.Size()

	// 2. write length prefix (4 bytes)
	lenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBytes, uint32(len(data)))

	// 3. write data
	if _, err := w.file.Write(lenBytes); err != nil {
		return 0, err
	}

	// 4. write actual data
	if _, err := w.file.Write(data); err != nil {
		return 0, err
	}

	// 5. sync to disk
	// Without this, data sits in OS RAM and is lost if power fails.
	if err := w.file.Sync(); err != nil {
		return 0, err
	}

	fmt.Printf("Wrote %d bytes at offset %d\n", len(data), offset)
	return offset, nil

}

// Read takes an offset and returns the data at that offset
func (w *WAL) Read(offset int64) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Jump to the specific byte offset
	_, err := w.file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	// 2. Read the length prefix (4 bytes)
	lenBytes := make([]byte, 4)
	if _, err := w.file.Read(lenBytes); err != nil {
		return nil, err
	}

	msgLen := binary.LittleEndian.Uint32(lenBytes)

	// 3. Read the actual message data
	data := make([]byte, msgLen)
	if _, err := w.file.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}

// Close safely closes the file handle
func (w *WAL) Close() error {
	return w.file.Close()
}
