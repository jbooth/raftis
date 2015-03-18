package raftis

import (
	"testing"
)

func TestAppend(t *testing.T) {
	setupTest()

	// put from server 1
	err := testcluster.clients[0].Set("append_test", "val1", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	// append from server 1
	_, err = testcluster.clients[0].Append("append_test", "APPENDED")
	if err != nil {
		t.Fatal(err)
	}
	// put from server 2 to impose happens-before
	err = testcluster.clients[1].Set("append_test2", "val2", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	// read from server 2
	val, err := testcluster.clients[1].Get("append_test")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val1APPENDED" {
		t.Fatalf("Expected [val1] for 'append_test', got [%s]", string(val))
	}
}
