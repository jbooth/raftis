package raftis

import (
	"testing"
)

func TestStrLen(t *testing.T) {
	setupTest()

	l, err := testcluster.clients[0].StrLen("strlen_test")
	if err != nil {
		t.Fatal(err)
	}
	if l != 0 {
		t.Fatal("strlen_test is not set yet, StrLen must be 0, got %s", l)
	}

	val := "value"
	testcluster.clients[0].Set("strlen_test", val, 0, 0, false, false)

	l, err = testcluster.clients[0].StrLen("strlen_test")
	if err != nil {
		t.Fatal(err)
	}
	if l != int64(len(val)) {
		t.Fatal("Expecting %d, but got %d", len(val), l)
	}

}
