package iterator

import (
	"encoding/binary"
	"io"
	"kvdb/types"
	"os"
)

type MemTableIterator struct {
	items []types.Item
	index int
}

func NewMemTableIterator(items []types.Item) (*MemTableIterator, error) {
	return &MemTableIterator{
		items: items,
		index: 0,
	}, nil
}

func (m *MemTableIterator) HasNext() bool {
	return m.index < len(m.items)
}

func (m *MemTableIterator) Next() (item types.Item, err error) {
	if !m.HasNext() {
		return
	}

	keyValue := m.items[m.index]
	m.index++

	return keyValue, nil
}

func (m *MemTableIterator) Close() error {
	return nil
}

type SstableIterator struct {
	file      *os.File
	positions []int64
	index     int
}

func NewSstableIterator(filename string) (*SstableIterator, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	positions, err := readIndexBlock(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &SstableIterator{
		file:      file,
		positions: positions,
		index:     0,
	}, nil
}

func readIndexBlock(file *os.File) ([]int64, error) {
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

func (s *SstableIterator) Next() (item types.Item, err error) {
	if !s.HasNext() {
		return
	}

	pos := s.positions[s.index]
	s.file.Seek(pos, io.SeekStart)

	item = types.Item{
		Key:   read(s.file),
		Value: read(s.file),
	}

	s.index++

	return item, nil
}

func (s *SstableIterator) HasNext() bool {
	return s.index < len(s.positions)-1
}

func (s *SstableIterator) Close() error {
	return s.file.Close()
}

func read(file *os.File) (value string) {
	var halfLen int32
	binary.Read(file, binary.LittleEndian, &halfLen)
	bytes := make([]byte, halfLen)
	file.Read(bytes)

	return string(bytes)
}
