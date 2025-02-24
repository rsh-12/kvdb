package lsm

import (
	"fmt"
	"kvdb/core/lsm/memtable"
	"kvdb/core/lsm/sstable"
	"kvdb/internal/config"
	"kvdb/internal/util"
	"log"
	"path/filepath"
)

type LSMTree struct {
	memTable  *memtable.MemTable
	sstables  []*sstable.SSTable
	threshold int
}

var storageDir string

func NewLSMTree(cfg *config.Config) *LSMTree {
	storageDir = getStorageDir(cfg.Core.StorageDir)
	// TODO: restore sstables

	return &LSMTree{
		memTable:  memtable.NewMemTable(),
		sstables:  make([]*sstable.SSTable, 0),
		threshold: cfg.Core.Threshold,
	}
}

func (l *LSMTree) Put(key, value string) {
	l.memTable.Put(key, value)

	if l.memTable.Len() >= l.threshold {
		filename := filepath.Join(storageDir, fmt.Sprintf("sstable_%d", len(l.sstables)))
		if err := l.memTable.Flush(filename); err != nil {
			log.Fatal("error flushing MemTable to sstable:", err)
			return
		}
		l.sstables = append(l.sstables, sstable.NewSSTable(filename))
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

func getStorageDir(storageDir string) string {
	projectDir := util.GetProjectDir()
	return filepath.Join(projectDir, storageDir)
}

func isTombstone(value string) bool {
	return value == ""
}
