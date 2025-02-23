package lsm

import (
	"encoding/binary"
	"io"
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

	var idxPos int64
	file.Seek(-8, io.SeekEnd)
	binary.Read(file, binary.LittleEndian, &idxPos)

	file.Seek(idxPos, io.SeekStart)
	var positions []int64
	for {
		var pos int64
		err := binary.Read(file, binary.LittleEndian, &pos)
		if err != nil {
			break
		}
		positions = append(positions, pos)
	}

	return binarySearch(positions, file, key)
}

func binarySearch(positions []int64, file *os.File, key string) (string, bool) {
	low, high := 0, len(positions)-1
	for low <= high {
		mid := (low + high) / 2
		file.Seek(positions[mid], io.SeekStart)

		readKey := read(file)
		if readKey == key {
			value, exists := read(file), true
			return value, exists
		} else if readKey < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return "", false
}

func read(file *os.File) (value string) {
	var halfLen int32
	binary.Read(file, binary.LittleEndian, &halfLen)
	bytes := make([]byte, halfLen)
	file.Read(bytes)

	return string(bytes)
}
