package memtable_test

import (
	"kvdb/core/lsm/memtable"
	"kvdb/core/errors"
	"kvdb/tests"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	memTable := memtable.NewMemTable()
	memTable.Put("profile", "dev")

	got, err := memTable.Get("profile")
	want := "dev"

	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestGet(t *testing.T) {
	memTable := memtable.NewMemTable()
	memTable.Put("profile", "local")

	got, err := memTable.Get("profile")
	want := "local"

	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestDelete(t *testing.T) {
	key := "profile"
	memTable := memtable.NewMemTable()
	memTable.Put(key, "prod")

	memTable.Delete(key)
	got, err := memTable.Get(key)

	assert.ErrorIs(t, err, errors.ErrTombstone)
	assert.Empty(t, got)
}

func TestFlush(t *testing.T) {
	memTable := memtable.NewMemTable()
	memTable.Put("profile", "local")

	err := memTable.Flush("/tmp/sstable")
	fileInfo, fileErr := os.Stat("/tmp/sstable")
	defer os.Remove(fileInfo.Name())

	assert.NoError(t, err)
	assert.NoError(t, fileErr)

	tests.ClearTestData()
}
