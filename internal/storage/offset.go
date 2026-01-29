package storage

import (
	"encoding/json"
	"os"
	"sync"
)

type OffsetManager struct {
	mu      sync.Mutex
	Offsets map[string]int
	file    string
}

func NewOffsetManager(path string) *OffsetManager {
	om := &OffsetManager{
		Offsets: make(map[string]int),
		file:    path + "/offsets.json",
	}
	om.load()
	return om
}

// SaveOffset updates the offset for a consumer group
func (om *OffsetManager) SaveOffset(group string, offset int) {
	om.mu.Lock()
	defer om.mu.Unlock()

	om.Offsets[group] = offset
	om.persist() // Save to disk immediately (Simple implementation)
}

// GetOffset returns the last saved offset (or 0 if new)
func (om *OffsetManager) GetOffset(group string) int {
	om.mu.Lock()
	defer om.mu.Unlock()
	return om.Offsets[group]
}

func (om *OffsetManager) persist() {
	data, _ := json.Marshal(om.Offsets)
	os.WriteFile(om.file, data, 0644)
}

func (om *OffsetManager) load() {
	data, err := os.ReadFile(om.file)
	if err == nil {
		json.Unmarshal(data, &om.Offsets)
	}
}
