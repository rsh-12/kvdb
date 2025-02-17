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

func binarySearch(positions []int64, file *os.File, key string) (value string, exists bool) {
	low, high := 0, len(positions)-1
	for low <= high {
		mid := (low + high) / 2
		file.Seek(positions[mid], io.SeekStart)

		readKey := readKey(file)
		if readKey == key {
			return readValue(file), true
		} else if readKey < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return
}

func readValue(file *os.File) (value string) {
	var valueLen int32
	binary.Read(file, binary.LittleEndian, &valueLen)
	valueBytes := make([]byte, valueLen)
	file.Read(valueBytes)

	return string(valueBytes)
}

func readKey(file *os.File) (key string) {
	var keyLen int32
	binary.Read(file, binary.LittleEndian, &keyLen)
	keyBytes := make([]byte, keyLen)
	file.Read(keyBytes)

	return string(keyBytes)
}
