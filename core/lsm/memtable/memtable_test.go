package memtable_test

import (
	"kvdb/core/lsm/memtable"
	"kvdb/tests"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	memTable := memtable.NewMemTable()
	memTable.Put("profile", "dev")

	got, _ := memTable.Get("profile")
	want := "dev"

	assert.Equal(t, want, got)

	tests.ClearTestData()
}

func TestGet(t *testing.T) {
	memTable := memtable.NewMemTable()
	memTable.Put("profile", "local")

	got, exists := memTable.Get("profile")
	want := "local"

	assert.Equal(t, want, got)
	assert.True(t, exists)

	tests.ClearTestData()
}

func TestDelete(t *testing.T) {
	key := "profile"
	memTable := memtable.NewMemTable()
	memTable.Put(key, "prod")

	memTable.Delete(key)
	got, exists := memTable.Get(key)

	assert.Empty(t, got)
	assert.True(t, exists)

	tests.ClearTestData()
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
