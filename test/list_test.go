package raftis

import (
	"bytes"
	"fmt"
	"github.com/xuyu/goredis"
	"strings"
	"testing"
)

func TestRPushAndLLen(t *testing.T) {
	setupTest()

	client := testcluster.clients[0]
	llen, err := client.LLen("rpush_llen_test")
	if err != nil {
		t.Fatal(err)
	}
	if llen != 0 {
		t.Fatalf("LLEN must return 0 for non-existing keys, but %d returned", llen)
	}

	total, err := client.RPush("rpush_llen_test", "a")
	if err != nil {
		t.Fatal(err)
	}

	if total != 1 {
		t.Fatalf("Expecting list to contain 1 element, but got %d", total)
	}

	llen, err = client.LLen("rpush_llen_test")
	if err != nil {
		t.Fatal(err)
	}
	if llen != 1 {
		t.Fatalf("Expecting list to contain 1 element, but got %d", llen)
	}

	total, err = client.RPush("rpush_llen_test", "b", "c")
	if err != nil {
		t.Fatal(err)
	}

	if total != 3 {
		t.Fatalf("Expecting list to contain 3 elements, but got %d", total)
	}

	llen, err = client.LLen("rpush_llen_test")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("LLEN is %d", llen)
	if llen != 3 {
		t.Fatalf("Expecting list to contain 3 elements, but got %d", llen)
	}

}

func TestLRange(t *testing.T) {
	setupTest()

	client := testcluster.clients[0]

	range_, err := client.LRange("lrange_test", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if range_ != nil {
		t.Fatalf("LRANGE for non-existing key must return nil, got %s", range_)
	}

	_, err = client.RPush("lrange_test", "a", "b", "c")
	if err != nil {
		t.Fatal(err)
	}

	range_, err = client.LRange("lrange_test", -1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if range_ != nil {
		t.Fatalf("LRANGE for negative start must return nil, got %s", range_)
	}

	range_, err = client.LRange("lrange_test", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if range_ != nil {
		t.Fatalf("LRANGE for end < start must return nil, got %s", range_)
	}

	range_, err = client.LRange("lrange_test", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(range_, " ") != "a b" {
		t.Fatalf("Expecting [a b], got %s", range_)
	}

	range_, err = client.LRange("lrange_test", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(range_, " ") != "a b c" {
		t.Fatalf("Expecting [a b c], got %s", range_)
	}

	range_, err = client.LRange("lrange_test", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(range_, " ") != "b" {
		t.Fatalf("Expecting [b], got %s", range_)
	}

}

func TestLPopRange(t *testing.T) {
	setupTest()

	client := testcluster.clients[0]

	resp, err := client.Eval("lpoprange", []string{"lpoprange_test"}, []string{"1", "2"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Bulk != nil {
		t.Fatalf("LPOPRANGE for non-existing key must return nil, got %s", resp.Bulk)
	}

	_, err = client.RPush("lpoprange_test", "a", "b", "c", "d", "e")
	if err != nil {
		t.Fatal(err)
	}

	resp, err = client.Eval("lpoprange", []string{"lpoprange_test"}, []string{"-1", "1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Bulk != nil {
		t.Fatalf("LRANGE for negative start must return nil, got %s", resp.Bulk)
	}

	resp, err = client.Eval("lpoprange", []string{"lpoprange_test"}, []string{"1", "0"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Bulk != nil {
		t.Fatalf("LRANGE for end < start must return nil, got %s", resp.Bulk)
	}

	range_, err := client.LRange("lpoprange_test", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(range_, " ") != "a b c d e" {
		t.Fatalf("Expecting [a b c d e], got [%s]", range_)
	}

	resp, err = client.Eval("lpoprange", []string{"lpoprange_test"}, []string{"1", "2"})
	if err != nil {
		t.Fatal(err)
	}

	result := collectMultiBulk(resp)
	if string(result) != "b c" {
		t.Fatalf("Expecting [b c], got [%s]", result)
	}

	range_, err = client.LRange("lpoprange_test", 0, 10)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Join(range_, " ") != "a d e" {
		t.Fatalf("Expecting [a d e], got [%s]", range_)
	}
}

func collectMultiBulk(resp *goredis.Reply) []byte {
	if resp.Type != 4 {
		panic("Expecting MultiBulk...")
	}
	answers := make([][]byte, len(resp.Multi))
	//	answers := make([]byte, 0)
	for i, r := range resp.Multi {
		answers[i] = r.Bulk
	}
	return bytes.Join(answers, []byte(" "))
}

// for debug
func printReply(resp *goredis.Reply) {
	println("RESP.Type", resp.Type)
	println("RESP.Error", resp.Error)
	println("RESP.Status", resp.Status)
	println("RESP.Integer", resp.Integer)
	println("RESP.Bulk", string(resp.Bulk))
	if resp.Multi != nil {
		println("RESP.Multi: ")
		for _, r := range resp.Multi {
			printReply(r)
		}
	}
}
