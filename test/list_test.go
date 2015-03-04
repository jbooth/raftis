package raftis

import (
	"fmt"
	"testing"
)

func TestRPushAndLLen(t *testing.T) {
	setupTest()

	llen, err := testcluster.clients[0].LLen("list_test")
	if err != nil {
		t.Fatal(err)
	}
	if llen != 0 {
		t.Fatalf("LLEN must return 0 for non-existing keys, but %d returned", llen)
	}

	total, err := testcluster.clients[0].RPush("list_test", "a")
	if err != nil {
		t.Fatal(err)
	}

	if total != 1 {
		t.Fatalf("Expecting list to contain 1 element, but got %d", total)
	}

	llen, err = testcluster.clients[0].LLen("list_test")
	if err != nil {
		t.Fatal(err)
	}
	if llen != 1 {
		t.Fatalf("Expecting list to contain 1 element, but got %d", llen)
	}

	total, err = testcluster.clients[0].RPush("list_test", "b", "c")
	if err != nil {
		t.Fatal(err)
	}

	if total != 3 {
		t.Fatalf("Expecting list to contain 3 elements, but got %d", total)
	}

	llen, err = testcluster.clients[0].LLen("list_test")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("LLEN is %d", llen)
	if llen != 3 {
		t.Fatalf("Expecting list to contain 3 elements, but got %d", llen)
	}

}
