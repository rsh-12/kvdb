package lsm_test

import (
	"os"
	"path/filepath"
	"testing"

	"kvdb/core/errors"
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

		value, err := lsm.Get("level")

		assert.NoError(t, err)
		assert.Equal(t, "debug", value)
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

		value, err := lsm.Get("config")

		assert.ErrorIs(t, err, errors.ErrNotFound)
		assert.Empty(t, value)
	})
}

func TestDelete(t *testing.T) {

	t.Run("deleting value from memtable", func(t *testing.T) {
		lsm := setUp(5, func(l *lsm.LSMTree) {
			l.Put("level", "warn")
		})

		lsm.Delete("level")
		value, err := lsm.Get("level")

		assert.ErrorIs(t, err, errors.ErrTombstone)
		assert.Empty(t, value)
	})

	t.Run("deleting value from sstable", func(t *testing.T) {
		lsm := setUp(1, func(l *lsm.LSMTree) {
			l.Put("level", "info")
		})

		lsm.Delete("level")
		value, err := lsm.Get("level")

		assert.ErrorIs(t, err, errors.ErrTombstone)
		assert.Empty(t, value)
	})
}

func TestPut(t *testing.T) {

	t.Run("inserting value after deletion", func(t *testing.T) {
		const key = "level"
		lsm := setUp(1, func(l *lsm.LSMTree) {
			l.Delete(key)
		})

		lsm.Put(key, "error")
		value, err := lsm.Get(key)

		assert.NoError(t, err)
		assert.Equal(t, "error", value)
	})

	tests.ClearTestData()
}

func TestFlush(t *testing.T) {
	lsm := setUp(10, func(l *lsm.LSMTree) {
		l.Put("level", "debug")
	})

	flushErr := lsm.Flush()
	assertFile(t, flushErr)

	value, err := lsm.Get("level")

	assert.NoError(t, err)
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
