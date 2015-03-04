package raftis

import (
	"testing"
)

func TestHashes(t *testing.T) {
	setupTest()

	key := "hashKey1"
	field := "field1"

	// put from server 1
	_, err := testcluster.clients[0].HSet(key, field, "val1")
	if err != nil {
		t.Fatal(err)
	}
	// overwrite from server 2 to impose happens-before
	_, err = testcluster.clients[1].HSet(key, field, "val2")
	if err != nil {
		t.Fatal(err)
	}
	// read from server 2
	val, err := testcluster.clients[1].HGet(key, field)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val2" {
		t.Fatalf("Expected 'val2' for '%s' '%s', got %s", key, field, string(val))
	}

}
