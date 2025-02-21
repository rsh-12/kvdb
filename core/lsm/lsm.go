package lsm

import (
	"fmt"
	"log"
	"os"
)

type LSMTree struct {
	memTable  *MemTable
	sstables  []*SSTable
	threshold int
}

func NewLSMTree(threshold int) *LSMTree {
	return &LSMTree{
		memTable:  NewMemTable(),
		sstables:  make([]*SSTable, 0),
		threshold: threshold,
	}
}

func (l *LSMTree) Put(key, value string) {
	l.memTable.Put(key, value)

	if len(l.memTable.data) >= l.threshold {
		pwd, _ := os.Getwd()
		filename := fmt.Sprintf("%v/core/lsm/data/sstable_%d", pwd, len(l.sstables))
		if err := l.memTable.Flush(filename); err != nil {
			log.Fatal("error flushing MemTable to sstable:", err)
			return
		}
		l.sstables = append(l.sstables, NewSSTable(filename))
	}
}

func (l *LSMTree) Get(key string) (value string, exists bool) {
	if value, exists := l.memTable.Get(key); exists {
		return value, true
	}

	for _, sstable := range l.sstables {
		if value, exists := sstable.Get(key); exists {
			return value, true
		}
	}

	return
}
