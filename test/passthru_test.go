package raftis

import (
	"fmt"
	"github.com/jbooth/raftis"
	"testing"
)

func TestPassthru(t *testing.T) {
	setupTest()

	// set up some data
	err := testcluster.clients[0].Set("one", "one", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	err = testcluster.clients[0].Set("two", "two", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	err = testcluster.clients[0].Set("three", "three", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}

	// passthru to a diff host
	testcluster.clients[1].Ping()
	fmt.Println("building passthru")
	passthru, err := raftis.NewPassThru(testcluster.redisAddrs[1])
	keys := []string{"one", "two", "three"}
	done := make(map[string]chan error)
	for _, key := range keys {
		fmt.Printf("dispatching GET for key %s\n", key)
		c := make(chan error, 1)
		done[key] = c
		go func(k string, onDone chan error) {
			fmt.Printf("sending GET %s\n", k)
			pendingResp, err := passthru.Command("GET", [][]byte{[]byte(k)})
			if err != nil {
				c <- err
				return
			}
			fmt.Printf("waiting resp %s\n", k)
			resp := <-pendingResp
			if resp.Err != nil {
				c <- err
				return
			}
			if string(resp.Data) != k {
				c <- err
				return
			}
			c <- nil
			return

		}(key, c)
	}
	fmt.Println("waiting all done")
	// wait all done
	for _, c := range done {
		err = <-c
		if err != nil {
			t.Fatal(err)
		}
	}
}
