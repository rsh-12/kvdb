package sstable_test

import (
	"os"
	"path/filepath"
	"testing"

	"kvdb/core/lsm/memtable"
	"kvdb/core/lsm/sstable"
	"kvdb/core/errors"
	"kvdb/internal/util"

	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {

	t.Run("existing value", func(t *testing.T) {
		sstable := setUp(func(mt *memtable.MemTable) {
			mt.Put("level", "info")
		})

		got, err := sstable.Get("level")
		want := "info"

		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("non-existent value", func(t *testing.T) {
		sstable := setUp(func(mt *memtable.MemTable) {})

		got, err := sstable.Get("config")

		assert.ErrorIs(t, err, errors.ErrNotFound)
		assert.Empty(t, got)
	})

	t.Run("existing tombstone value", func(t *testing.T) {
		sstable := setUp(func(mt *memtable.MemTable) {
			mt.Delete("level")
		})

		value, err := sstable.Get("level")

		assert.ErrorIs(t, err, errors.ErrTombstone)
		assert.Empty(t, value)
	})

	clear()
}

func setUp(data func(*memtable.MemTable)) *sstable.SSTable {
	const path = "/tmp/sstable"
	memTable := memtable.NewMemTable()
	data(memTable)
	memTable.Flush(path)
	return sstable.NewSSTable(path)
}

func clear() {
	os.RemoveAll(filepath.Join(util.GetProjectDir(), "test_data"))
}
