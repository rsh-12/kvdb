package memtable

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemTable(t *testing.T) {

	t.Run("Put", func(t *testing.T) {
		memTable := NewMemTable()
		memTable.Put("profile", "dev")

		got := memTable.data["profile"]
		want := "dev"

		assert.Equal(t, want, got)
	})

	t.Run("Get", func(t *testing.T) {
		memTable := NewMemTable()
		memTable.Put("profile", "local")

		got, exists := memTable.Get("profile")
		want := "local"

		assert.Equal(t, want, got)
		assert.True(t, exists)
	})

	t.Run("Delete", func(t *testing.T) {
		key := "profile"
		memTable := NewMemTable()
		memTable.Put(key, "prod")

		memTable.Delete(key)

		got, exists := memTable.Get(key)
		want := ""

		assert.Equal(t, want, got)
		assert.True(t, exists)
	})

	t.Run("Flush", func(t *testing.T) {
		memTable := NewMemTable()
		memTable.Put("profile", "local")

		err := memTable.Flush("/tmp/sstable")
		fileInfo, fileErr := os.Stat("/tmp/sstable")
		defer os.Remove(fileInfo.Name())

		assert.NoError(t, err)
		assert.NoError(t, fileErr)
	})
}
