package lsm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSstable(t *testing.T) {
	setUp := func(data func(*MemTable)) *SSTable {
		const path = "/tmp/sstable"
		memTable := NewMemTable()
		data(memTable)
		memTable.Flush(path)
		return NewSSTable(path)
	}

	t.Run("value exists", func(t *testing.T) {
		sstable := setUp(func(mt *MemTable) {
			mt.Put("level", "info")
		})

		got, exists := sstable.Get("level")
		want := "info"

		assert.True(t, exists)
		assert.Equal(t, want, got)
	})

	t.Run("value not found", func(t *testing.T) {
		sstable := setUp(func(mt *MemTable) {})

		got, exists := sstable.Get("config")

		assert.False(t, exists)
		assert.Empty(t, got)
	})

	t.Run("value exists, but marked as deleted", func(t *testing.T) {
		sstable := setUp(func(mt *MemTable) {
			mt.Delete("level")
		})

		value, exists := sstable.Get("level")

		assert.True(t, exists)
		assert.Empty(t, value)
	})

}
