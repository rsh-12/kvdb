package util

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"
)

func GetProjectDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("could not get current path:", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		if dir == "/" {
			log.Fatal("could not find project root path:", err)
		}
		dir = filepath.Dir(dir)
	}
}

func ReadBytes(file *os.File) (value string) {
	var halfLen int32
	binary.Read(file, binary.LittleEndian, &halfLen)
	bytes := make([]byte, halfLen)
	file.Read(bytes)
	return string(bytes)
}

func ReadIndexBlock(file *os.File) ([]int64, error) {
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

	return positions, nil
}
