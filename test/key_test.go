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

func TestDel(t *testing.T) {
	setupTest()

	del, err := testcluster.clients[0].Del("delete_test1", "delete_test2", "delete_test3")
	if err != nil {
		t.Fatal(err)
	}
	if del != 0 {
		t.Fatalf("Del must return 0 for non-existing keys, but it insists it deleted %d", del)
	}

	testcluster.clients[0].Set("delete_test1", "a", 0, 0, false, false)
	testcluster.clients[0].Set("delete_test2", "b", 0, 0, false, false)
	testcluster.clients[0].Set("delete_test3", "c", 0, 0, false, false)

	del, err = testcluster.clients[0].Del("delete_test1", "delete_testX")
	if err != nil {
		t.Fatal(err)
	}
	if del != 1 {
		t.Fatalf("Expecting Del to delete 1 key, deleted %d", del)
	}

	// we don't support multiple deletes
	//del, err = testcluster.clients[0].Del("delete_test1", "delete_test2", "delete_test3")
	//if err != nil {
	//t.Fatal(err)
	//}
	//if del != 2 {
	//t.Fatalf("Expecting Del to delete 2 keys, deleted %d", del)
	//}
}
