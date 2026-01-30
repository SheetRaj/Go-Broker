package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WAL struct {
	dir         string
	activeFile  *os.File
	activeSize  int64
	maxFileSize int64
	mu          sync.Mutex
}

// NewWAL scans the directory and opens the correct active log file.
func NewWAL(dataDir string, topic string, maxFileSize int64) (*WAL, error) {
	path := filepath.Join(dataDir, topic)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	wal := &WAL{
		dir:         path,
		maxFileSize: maxFileSize,
	}

	// 1. Find the latest log file to be the "Active" one
	files, _ := os.ReadDir(path)
	var startOffsets []int
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".log") {
			// Filename format: "0.log", "1024.log"
			name := strings.TrimSuffix(f.Name(), ".log")
			offset, _ := strconv.Atoi(name)
			startOffsets = append(startOffsets, offset)
		}
	}
	sort.Ints(startOffsets)

	// Default to 0.log if empty
	lastOffset := 0
	if len(startOffsets) > 0 {
		lastOffset = startOffsets[len(startOffsets)-1]
	}

	// 2. Open that file
	if err := wal.openActiveFile(lastOffset); err != nil {
		return nil, err
	}

	// 3. Start Background Cleanup (Retention Policy)
	go wal.startCleaner()

	return wal, nil
}

func (w *WAL) openActiveFile(startOffset int) error {
	filename := filepath.Join(w.dir, fmt.Sprintf("%d.log", startOffset))
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	stat, _ := f.Stat()
	w.activeFile = f
	w.activeSize = stat.Size()
	return nil
}

// rotate closes the current file and opens a new one at the current global offset.
func (w *WAL) rotate() error {
	// 1. Calculate new filename based on current total size/offset
	// (Simplification: We use the file's current end offset as the new name)
	// Get current file name to parse starting offset
	stat, _ := w.activeFile.Stat()
	baseName := strings.TrimSuffix(stat.Name(), ".log")
	startOffset, _ := strconv.Atoi(baseName)

	newOffset := int64(startOffset) + w.activeSize

	fmt.Printf("Rotating! Log full. Closing %s, opening %d.log\n", stat.Name(), newOffset)

	// 2. Close old
	w.activeFile.Close()

	// 3. Open new
	return w.openActiveFile(int(newOffset))
}

func (w *WAL) Write(data []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Check if we need to rotate
	if w.activeSize >= w.maxFileSize {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}

	// 2. Calculate Global Offset (Start of File + Position in File)
	stat, _ := w.activeFile.Stat()
	baseName := strings.TrimSuffix(stat.Name(), ".log")
	fileStartOffset, _ := strconv.Atoi(baseName)

	currentGlobalOffset := int64(fileStartOffset) + w.activeSize

	// 3. Prepare Data
	lenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBytes, uint32(len(data)))

	// 4. Write to Disk
	if _, err := w.activeFile.Write(lenBytes); err != nil {
		return 0, err
	}
	if _, err := w.activeFile.Write(data); err != nil {
		return 0, err
	}
	w.activeFile.Sync()

	// 5. Update In-Memory Size
	entrySize := int64(4 + len(data))
	w.activeSize += entrySize

	return currentGlobalOffset, nil
}

func (w *WAL) Read(offset int64) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Find which file contains this offset
	// Brute force: Check all files (In prod, keep an index in memory)
	files, _ := os.ReadDir(w.dir)
	var segmentFile string
	var segmentStart int64

	// We need to find the file where: StartOffset <= TargetOffset
	// And pick the *largest* StartOffset that fits that rule.
	bestStart := int64(-1)

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".log") {
			name := strings.TrimSuffix(f.Name(), ".log")
			start, _ := strconv.Atoi(name)
			start64 := int64(start)

			if start64 <= offset && start64 > bestStart {
				bestStart = start64
				segmentFile = f.Name()
				segmentStart = start64
			}
		}
	}

	if segmentFile == "" {
		return nil, fmt.Errorf("offset not found")
	}

	// 2. Open the file (Read Only)
	path := filepath.Join(w.dir, segmentFile)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// 3. Seek to local position (Target - FileStart)
	localOffset := offset - segmentStart
	_, err = f.Seek(localOffset, 0)
	if err != nil {
		return nil, err
	}

	// 4. Read Size & Data
	lenBytes := make([]byte, 4)
	if _, err := f.Read(lenBytes); err == io.EOF {
		return nil, fmt.Errorf("EOF")
	}
	msgLen := binary.LittleEndian.Uint32(lenBytes)

	data := make([]byte, msgLen)
	if _, err := f.Read(data); err != nil {
		return nil, err
	}

	return data, nil
}

func (w *WAL) Close() error {
	return w.activeFile.Close()
}

// startCleaner runs in the background and deletes old files
func (w *WAL) startCleaner() {
	ticker := time.NewTicker(10 * time.Second) // Check every 10 seconds
	for range ticker.C {
		w.mu.Lock()
		files, _ := os.ReadDir(w.dir)

		// If we only have 1 file, don't delete it (even if old)
		if len(files) <= 1 {
			w.mu.Unlock()
			continue
		}

		for _, f := range files {
			if !strings.HasSuffix(f.Name(), ".log") {
				continue
			}

			// Don't delete the active file!
			stat, _ := w.activeFile.Stat()
			if f.Name() == stat.Name() {
				continue
			}

			// Check Age
			info, _ := f.Info()
			age := time.Since(info.ModTime())

			// Retention Policy: Delete if older than 2 minutes (For testing)
			// In Prod, change this to: if age > 7 * 24 * time.Hour
			if age > 2*time.Minute {
				fmt.Printf("Retention: Deleting old segment %s\n", f.Name())
				os.Remove(filepath.Join(w.dir, f.Name()))
			}
		}
		w.mu.Unlock()
	}
}
