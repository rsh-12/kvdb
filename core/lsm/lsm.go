package lsm

import (
	"fmt"
	"io/fs"
	"kvdb/core/lsm/memtable"
	"kvdb/core/lsm/sstable"
	"kvdb/internal/config"
	"kvdb/internal/util"
	"kvdb/types"
	"log"
	"os"
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
	sstables := restoreSstables()

	return &LSMTree{
		memTable:  memtable.NewMemTable(),
		sstables:  sstables,
		threshold: cfg.Core.Threshold,
	}
}

func restoreSstables() []*sstable.SSTable {
	var sstables []*sstable.SSTable

	err := filepath.Walk(storageDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Fatalf("error accessing path %q: %v\n", path, err)
			return nil
		}
		if !info.IsDir() {
			sstables = append(sstables, sstable.NewSSTable(path))
		}
		return nil
	})

	if err != nil {
		log.Fatalf("error restoring sstables: %v\n", err)
	}

	return sstables
}

func (l *LSMTree) Put(key, value string) {
	l.memTable.Put(key, value)

	if l.memTable.Len() >= l.threshold {
		flush(l)
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

func (l *LSMTree) Flush() error {
	if l.memTable.Len() == 0 {
		return nil
	}
	return flush(l)
}

func flush(l *LSMTree) error {
	filename := filepath.Join(storageDir, fmt.Sprintf("sstable_%d", len(l.sstables)))
	if err := l.memTable.Flush(filename); err != nil {
		log.Fatal("error flushing MemTable to sstable:", err)
		return err
	}

	l.sstables = append(l.sstables, sstable.NewSSTable(filename))
	return nil
}

func getStorageDir(storageDir string) string {
	projectDir := util.GetProjectDir()
	storage := filepath.Join(projectDir, storageDir)

	if _, err := os.Stat(storage); os.IsNotExist(err) {
		err := os.MkdirAll(storage, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
	return storage
}

func isTombstone(value string) bool {
	return value == ""
}

func (l *LSMTree) OpenIterators() ([]types.Iterator, error) {
	iterators := make([]types.Iterator, 0, len(l.sstables)+1)

	// add memtable iterator first
	memIterator, err := l.memTable.Iterator()
	if err != nil {
		return nil, err
	}
	iterators = append(iterators, memIterator)

	// add sstable iterators in reverse order
	for i := len(l.sstables) - 1; i >= 0; i-- {
		sstIterator, err := l.sstables[i].Iterator()
		if err != nil {
			close(iterators)
			return nil, err
		}
		iterators = append(iterators, sstIterator)
	}

	return iterators, nil
}

func close(iterators []types.Iterator) {
	for _, it := range iterators {
		it.Close()
	}
}
