package lsm

import (
	"encoding/binary"
	"io"
	"os"
	"sort"
	"sync"
)

type MemTable struct {
	mutex sync.Mutex
	data  map[string]string
}

func NewMemTable() *MemTable {
	return &MemTable{
		data: make(map[string]string),
	}
}

func (m *MemTable) Put(key, value string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.data[key] = value
}

func (m *MemTable) Get(key string) (string, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	value, exists := m.data[key]
	if value == "" {
		return "", false
	}
	return value, exists
}

// Delete marks a key as deleted by inserting an empty value.
// Since the storage follows an append-only log approach, deletions are
// handled by writing a special empty value (acting as a tombstone)
// instead of removing the key directly.
// This ensures the deletion is recorded and can be processed during compaction.
func (m *MemTable) Delete(key string) {
	m.Put(key, "")
}

func (m *MemTable) Flush(filename string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	defer file.Close()

	var indices []int64
	for _, key := range keys {
		pos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		indices = append(indices, pos)

		binary.Write(file, binary.LittleEndian, int32(len(key)))
		file.Write([]byte(key))

		value := m.data[key]
		binary.Write(file, binary.LittleEndian, int32(len(value)))
		file.Write([]byte(value))
	}

	idxPos, _ := file.Seek(0, io.SeekCurrent)
	for _, pos := range indices {
		binary.Write(file, binary.LittleEndian, pos)
	}

	binary.Write(file, binary.LittleEndian, idxPos)

	m.data = make(map[string]string)
	return nil
}
