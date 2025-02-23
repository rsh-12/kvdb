package lsm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLsm(t *testing.T) {
	setUp := func(threshold int, enrich func(*LSMTree)) *LSMTree {
		lsm := NewLSMTree(threshold)
		enrich(lsm)
		return lsm
	}

	t.Run("value exists in memtable", func(t *testing.T) {
		lsm := setUp(10, func(l *LSMTree) {
			l.Put("level", "debug")
		})

		value, exists := lsm.Get("level")

		assert.Equal(t, "debug", value)
		assert.True(t, exists)
	})

	t.Run("value exists in sstable", func(t *testing.T) {
		lsm := setUp(1, func(l *LSMTree) {
			l.Put("level", "info")
		})

		got, _ := lsm.Get("level")
		want := "info"

		assert.Equal(t, want, got)
	})

	t.Run("value doesn't exist in sstable", func(t *testing.T) {
		lsm := setUp(1, func(l *LSMTree) {
			l.Put("level", "info")
		})

		value, exists := lsm.Get("config")

		assert.False(t, exists)
		assert.Empty(t, value)
	})

	t.Run("delete value from MemTable", func(t *testing.T) {
		lsm := setUp(5, func(l *LSMTree) {
			l.Put("level", "warn")
		})

		lsm.Delete("level")
		value, exists := lsm.Get("level")

		assert.Empty(t, value)
		assert.False(t, exists)
	})

	t.Run("get deleted value", func(t *testing.T) {
		lsm := setUp(1, func(l *LSMTree) {
			l.Put("level", "info")
		})

		lsm.Delete("level")
		value, exists := lsm.Get("level")

		assert.Empty(t, value)
		assert.False(t, exists)
	})

	t.Run("put value after deletion", func(t *testing.T) {
		const key = "level"
		lsm := setUp(1, func(l *LSMTree) {
			l.Delete(key)
		})

		lsm.Put(key, "error")
		value, exists := lsm.Get(key)

		assert.Equal(t, "error", value)
		assert.True(t, exists)
	})
}
