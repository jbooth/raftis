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

	//	// test some column PUTs and a get
	//	http.Get("http://localhost:8001/putCols/table1/row1?col1=val1&col2=val2")
	//	http.Get("http://localhost:8002/putCols/table1/row1?col3=val3&col4=val4")
	//
	//	row,err := http.Get("http://localHost:8003/getRow/table1/row1")
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//	fmt.Print(row)
	//
	//	// test get of bad key
	//	row,err = http.Get("http://localHost:8003/getRow/table1/badrow")
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//	fmt.Print(row)

	// shut'em down
	//for i := 0; i < 3; i++ {
	//fmt.Printf("Killing server %d\n", i)
	//dbs[i].Close()
	//}

}
