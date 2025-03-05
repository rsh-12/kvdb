package lsm_test

import (
	"os"
	"path/filepath"
	"testing"

	"kvdb/core/lsm"
	"kvdb/internal/config"
	"kvdb/internal/util"
	"kvdb/tests"

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

	tests.ClearTestData()
}

func TestFlush(t *testing.T) {
	lsm := setUp(10, func(l *lsm.LSMTree) {
		l.Put("level", "debug")
	})

	err := lsm.Flush()

	assertFile(t, err)

	value, exists := lsm.Get("level")
	assert.True(t, exists)
	assert.Equal(t, "debug", value)
}

func assertFile(t *testing.T, err error) {
	assert.Nil(t, err)
	sstablePath := filepath.Join(util.GetProjectDir(), "test_data", "sstable_0")
	_, err = os.Stat(sstablePath)
	assert.Nil(t, err)
}

func setUp(threshold int, enrich func(*lsm.LSMTree)) *lsm.LSMTree {
	properties := filepath.Join(util.GetProjectDir(), "config/test.yaml")
	os.Setenv("CONFIG_PATH", properties)

	cfg := config.MustLoad()
	cfg.SetThreshold(threshold)

	lsm := lsm.NewLSMTree(cfg)
	enrich(lsm)
	return lsm
}
