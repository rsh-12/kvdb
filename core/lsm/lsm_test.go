package lsm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	lsm := NewLSMTree(2)
	lsm.Put("level", "info")
	lsm.Put("profile", "dev")

	t.Run("value exists", func(t *testing.T) {
		got, _ := lsm.Get("level")
		want := "info"
		assert.Equal(t, want, got)
	})

	t.Run("value doesn't exist", func(t *testing.T) {
		_, exists := lsm.Get("config")
		assert.False(t, exists)
	})

}
