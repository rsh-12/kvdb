package lsm_test

import (
	"testing"

	"kvdb/core/lsm"

	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {

	t.Run("existing in memtable value", func(t *testing.T) {
		lsm := setUp(10, func(l *lsm.LSMTree) {
			l.Put("level", "debug")
		})

		value, exists := lsm.Get("level")

		assert.Equal(t, "debug", value)
		assert.True(t, exists)
	})

	t.Run("existing in sstable value", func(t *testing.T) {
		lsm := setUp(1, func(l *lsm.LSMTree) {
			l.Put("level", "info")
		})

		got, _ := lsm.Get("level")
		want := "info"

		assert.Equal(t, want, got)
	})

	t.Run("non-existent in sstable value", func(t *testing.T) {
		lsm := setUp(1, func(l *lsm.LSMTree) {
			l.Put("level", "info")
		})

		value, exists := lsm.Get("config")

		assert.False(t, exists)
		assert.Empty(t, value)
	})
}

func TestDelete(t *testing.T) {

	t.Run("deleting value from memtable", func(t *testing.T) {
		lsm := setUp(5, func(l *lsm.LSMTree) {
			l.Put("level", "warn")
		})

		lsm.Delete("level")
		value, exists := lsm.Get("level")

		assert.Empty(t, value)
		assert.False(t, exists)
	})

	t.Run("deleting value from sstable", func(t *testing.T) {
		lsm := setUp(1, func(l *lsm.LSMTree) {
			l.Put("level", "info")
		})

		lsm.Delete("level")
		value, exists := lsm.Get("level")

		assert.Empty(t, value)
		assert.False(t, exists)
	})
}

func TestPut(t *testing.T) {

	t.Run("inserting value after deletion", func(t *testing.T) {
		const key = "level"
		lsm := setUp(1, func(l *lsm.LSMTree) {
			l.Delete(key)
		})

		lsm.Put(key, "error")
		value, exists := lsm.Get(key)

		assert.Equal(t, "error", value)
		assert.True(t, exists)
	})
}

func setUp(threshold int, enrich func(*lsm.LSMTree)) *lsm.LSMTree {
	lsm := lsm.NewLSMTree(threshold)
	enrich(lsm)
	return lsm
}
