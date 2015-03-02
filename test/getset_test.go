package raftis

import (
	"testing"
)

func TestGetSet(t *testing.T) {
	setupTest()

	// put from server 1
	err := testcluster.clients[0].Set("key1", "val1", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	// put from server 2 to impose happens-before
	err = testcluster.clients[1].Set("key2", "val2", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	// check put from server 1, read from server 2
	val, err := testcluster.clients[1].Get("key1")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val1" {
		t.Fatalf("Expected 'val1' for 'key1', got %s", string(val))
	}
}
