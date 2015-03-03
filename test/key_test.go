package raftis

import (
	"testing"
)

func TestExists(t *testing.T) {
	setupTest()

	exs, err := testcluster.clients[0].Exists("exists_test")
	if err != nil {
		t.Fatal(err)
	}
	if exs {
		t.Fatalf("exists_test is not set yet, Exists must return 0, got %s", exs)
	}

	testcluster.clients[0].Set("exists_test", "whatever", 0, 0, false, false)

	exs, err = testcluster.clients[0].Exists("exists_test")
	if err != nil {
		t.Fatal(err)
	}
	if !exs {
		t.Fatalf("Expecting exists_test to exist, but looks like it doesn't (return %d)", exs)
	}
}
