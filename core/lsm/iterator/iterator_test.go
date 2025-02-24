package iterator_test

import (
	"kvdb/core/lsm/iterator"
	"kvdb/core/lsm/memtable"
	"kvdb/tests"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSstableIterator(t *testing.T) {
	memtable := memtable.NewMemTable()
	memtable.Put("level", "info")
	memtable.Put("profile", "staging")
	memtable.Flush("/tmp/sstable")

	iter, err := iterator.NewSstableIterator("/tmp/sstable")
	assert.Nil(t, err)

	hasNext := iter.HasNext()
	assert.True(t, hasNext)

	item, err := iter.Next()

	assert.Nil(t, err)
	assert.Equal(t, "level", item.Key)
	assert.Equal(t, "info", item.Value)

	item, err = iter.Next()
	assert.Nil(t, err)
	assert.Equal(t, "profile", item.Key)
	assert.Equal(t, "staging", item.Value)

	assert.False(t, iter.HasNext())

	tests.ClearTestData()
}
