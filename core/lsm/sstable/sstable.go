package sstable

import (
	"io"
	"kvdb/core/lsm/iterator"
	"kvdb/core/errors"
	"kvdb/internal/util"
	"kvdb/types"
	"os"
)

type SSTable struct {
	filename string
}

func NewSSTable(filename string) *SSTable {
	return &SSTable{filename: filename}
}

func (s *SSTable) Get(key string) (string, error) {
	file, err := os.Open(s.filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	positions, err := util.ReadIndexBlock(file)
	if err != nil {
		return "", err
	}

	value, err := binarySearch(positions, file, key)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", errors.ErrTombstone
	}
	return value, nil
}

func binarySearch(positions []int64, file *os.File, key string) (string, error) {
	low, high := 0, len(positions)-1
	for low <= high {
		mid := (low + high) / 2
		file.Seek(positions[mid], io.SeekStart)

		readKey := util.ReadBytes(file)
		if readKey == key {
			return util.ReadBytes(file), nil
		} else if readKey < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return "", errors.ErrNotFound
}

func (s *SSTable) Iterator() (types.Iterator, error) {
	return iterator.NewSstableIterator(s.filename)
}
