package main

import (
	"kvdb/core/lsm"
	"testing"
)

func TestGet(t *testing.T) {
	lsm := lsm.NewLSMTree(2)
	lsm.Put("level", "info")
	lsm.Put("profile", "dev")

	t.Run("value exists", func(t *testing.T) {
		got, _ := lsm.Get("level")
		want := "info"

		if got != want {
			t.Errorf("got %q want %q", got, want)
		}
	})

	t.Run("value doesn't exist", func(t *testing.T) {
		_, got := lsm.Get("config")
		want := false

		if got != want {
			t.Errorf("got %v want %v", got, want)
		}
	})

}
