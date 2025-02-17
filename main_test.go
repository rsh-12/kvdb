package main

import (
	"kvdb/lsm"
	"testing"
)

func TestGet(t *testing.T) {
	lsm := lsm.NewLSMTree(2)
	lsm.Put("level", "info")
	lsm.Put("profile", "dev")

	got, _ := lsm.Get("level")
	want := "info"

	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}
