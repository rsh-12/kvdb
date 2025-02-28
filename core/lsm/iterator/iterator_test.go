package iterator_test

import (
	"kvdb/core/lsm/iterator"
	"kvdb/core/lsm/memtable"
	"kvdb/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSstableIterator(t *testing.T) {
	path := "/tmp/sstable"
	memtable := setUp()
	memtable.Flush(path)

	iter, err := iterator.NewSstableIterator(path)

	assert.Nil(t, err)
	assertIterator(t, iter, []types.Item{
		{Key: "level", Value: "info"},
		{Key: "profile", Value: "staging"},
	})
}

func TestMemtableIterator(t *testing.T) {
	memtable := setUp()

	iter, err := memtable.Iterator()

	assert.Nil(t, err)
	assertIterator(t, iter, []types.Item{
		{Key: "level", Value: "info"},
		{Key: "profile", Value: "staging"},
	})
}

func setUp() *memtable.MemTable {
	memtable := memtable.NewMemTable()
	memtable.Put("level", "info")
	memtable.Put("profile", "staging")
	return memtable
}

func assertIterator(t testing.TB, iter types.Iterator, expectedItems []types.Item) {
	t.Helper()
	for _, expected := range expectedItems {
		assert.True(t, iter.HasNext())
		item, err := iter.Next()
		assert.Nil(t, err)
		assert.Equal(t, expected.Key, item.Key)
		assert.Equal(t, expected.Value, item.Value)
	}
	assert.False(t, iter.HasNext())
}
