package raftis

import (
	"testing"
	//"time"
)

func TestCluster(t *testing.T) {
	setupTest()

	// set up some data
	err := testcluster.clients[0].Set("foo", "bar", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	err = testcluster.clients[0].Ping()
	if err != nil {
		t.Fatalf("Error pinging client %s", err)
	}

	//for i := 1; i < 9; i++ {
	//err = testcluster.clients[i].Ping()
	//if err != nil {
	//t.Fatalf("Error pinging client %d : %s", i, err)
	//}
	//}
	//time.Sleep(1 * time.Second)

	for i := 1; i < 9; i++ {
		err = testcluster.clients[i].Ping()
		if err != nil {
			t.Fatalf("Error pinging client %d : %s", i, err)
		}
		v, err := testcluster.clients[i].Get("foo")
		if err != nil {
			t.Fatalf("Error GETing from client %d : %s", i, err)
		}
		if string(v) != "bar" {
			t.Fatalf("Wrong value GETing from client %d : got %s expected \"bar\"", i, string(v))
		}
	}

}
