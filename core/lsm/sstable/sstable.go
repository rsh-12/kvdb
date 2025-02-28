package sstable

import (
	"io"
	"kvdb/core/lsm/iterator"
	"kvdb/internal/util"
	"kvdb/types"
	"log"
	"os"
)

type SSTable struct {
	filename string
}

func NewSSTable(filename string) *SSTable {
	return &SSTable{filename: filename}
}

func (s *SSTable) Get(key string) (value string, exists bool) {
	file, err := os.Open(s.filename)
	if err != nil {
		return
	}
	defer file.Close()

	positions, err := util.ReadIndexBlock(file)
	if err != nil {
		file.Close()
		log.Fatal("error reading index block:", err)
	}

	return binarySearch(positions, file, key)
}

func binarySearch(positions []int64, file *os.File, key string) (string, bool) {
	low, high := 0, len(positions)-1
	for low <= high {
		mid := (low + high) / 2
		file.Seek(positions[mid], io.SeekStart)

		readKey := util.ReadBytes(file)
		if readKey == key {
			value, exists := util.ReadBytes(file), true
			return value, exists
		} else if readKey < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return "", false
}

func (s *SSTable) Iterator() (types.Iterator, error) {
	return iterator.NewSstableIterator(s.filename)
}
