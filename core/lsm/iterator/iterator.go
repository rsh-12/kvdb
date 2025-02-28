package iterator

import (
	"io"
	"kvdb/internal/util"
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

	positions, err := util.ReadIndexBlock(file)
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

func (s *SstableIterator) Next() (item types.Item, err error) {
	if !s.HasNext() {
		return
	}

	pos := s.positions[s.index]
	s.file.Seek(pos, io.SeekStart)

	item = types.Item{
		Key:   util.ReadBytes(s.file),
		Value: util.ReadBytes(s.file),
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
