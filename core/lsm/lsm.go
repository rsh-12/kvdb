package lsm

import (
	"fmt"
	"kvdb/internal/util"
	"log"
)

type LSMTree struct {
	memTable  *MemTable
	sstables  []*SSTable
	threshold int
}

var storageDir string

func NewLSMTree(threshold int) *LSMTree {
	storageDir = getStorageDir()
	// TODO: restore sstables

	return &LSMTree{
		memTable:  NewMemTable(),
		sstables:  make([]*SSTable, 0),
		threshold: threshold,
	}
}

func (l *LSMTree) Put(key, value string) {
	l.memTable.Put(key, value)

	if len(l.memTable.data) >= l.threshold {
		filename := fmt.Sprintf("%v/sstable_%d", storageDir, len(l.sstables))
		if err := l.memTable.Flush(filename); err != nil {
			log.Fatal("error flushing MemTable to sstable:", err)
			return
		}
		l.sstables = append(l.sstables, NewSSTable(filename))
	}
}

func (l *LSMTree) Get(key string) (string, bool) {
	// try to get the value from MemTable first
	if value, exists := l.memTable.Get(key); exists {
		if isTombstone(value) {
			return "", false
		}
		return value, true
	}

	return l.searchInSstables(key)
}

func (l *LSMTree) searchInSstables(key string) (string, bool) {
	for i := len(l.sstables) - 1; i >= 0; i-- {
		if value, exists := l.sstables[i].Get(key); exists {
			if isTombstone(value) {
				return "", false
			}
			return value, true
		}
	}
	return "", false
}

func (l *LSMTree) Delete(key string) {
	l.Put(key, "")
}

func getStorageDir() string {
	projectDir := util.GetProjectDir()
	storageDir := fmt.Sprintf("%v/core/lsm/data", projectDir)
	return storageDir
}

func isTombstone(value string) bool {
	return value == ""
}
