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

var projectDir string

func NewLSMTree(threshold int) *LSMTree {
	projectDir = util.GetProjectDir()

	return &LSMTree{
		memTable:  NewMemTable(),
		sstables:  make([]*SSTable, 0),
		threshold: threshold,
	}
}

func (l *LSMTree) Put(key, value string) {
	l.memTable.Put(key, value)

	if len(l.memTable.data) >= l.threshold {
		filename := fmt.Sprintf("%v/core/lsm/data/sstable_%d", projectDir, len(l.sstables))
		if err := l.memTable.Flush(filename); err != nil {
			log.Fatal("error flushing MemTable to sstable:", err)
			return
		}
		l.sstables = append(l.sstables, NewSSTable(filename))
	}
}

func (l *LSMTree) Get(key string) (string, bool) {
	if value, exists := l.memTable.Get(key); exists {
		return value, true
	}

	for _, sstable := range l.sstables {
		if value, exists := sstable.Get(key); exists {
			return value, true
		}
	}

	return "", false
}
