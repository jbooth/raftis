package raftis

import (
	"testing"
)

func TestSADDAndSCARD(t *testing.T) {
	setupTest()

	client := testcluster.clients[0]

	scard, err := client.SCard("sadd_scard_test")
	if err != nil {
		t.Fatal(err)
	}
	if scard != 0 {
		t.Fatalf("SCARD must return 0 for non-existing keys, but %d returned", scard)
	}

	added, err := client.SAdd("sadd_scard_test", "a")
	if err != nil {
		t.Fatal(err)
	}

	if added != 1 {
		t.Fatalf("Expecting add 1 element, but got %d", added)
	}

	scard, err = client.SCard("sadd_scard_test")
	if err != nil {
		t.Fatal(err)
	}
	if scard != 1 {
		t.Fatalf("Expecting set to contain 1 element, but got %d", scard)
	}

	added, err = client.SAdd("sadd_scard_test", "b", "c")
	if err != nil {
		t.Fatal(err)
	}

	if added != 2 {
		t.Fatalf("Expecting add 2 elements, but got %d", added)
	}

	scard, err = client.SCard("sadd_scard_test")
	if err != nil {
		t.Fatal(err)
	}
	if scard != 3 {
		t.Fatalf("Expecting list to contain 3 elements, but got %d", scard)
	}

	// adding duplicates
	added, err = client.SAdd("sadd_scard_test", "b", "c")
	if err != nil {
		t.Fatal(err)
	}

	if added != 0 {
		t.Fatalf("Expecting add 0 elements, but got %d", added)
	}

	scard, err = client.SCard("sadd_scard_test")
	if err != nil {
		t.Fatal(err)
	}
	if scard != 3 {
		t.Fatalf("Expecting list to contain 3 elements, but got %d", scard)
	}

}
