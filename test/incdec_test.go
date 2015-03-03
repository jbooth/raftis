package raftis

import (
  "testing"
)


func IncrTest(t *testing.T) {
  setupTest()
  key := "test_inc_key1"

  err := testcluster.clients[0].Set(key, "10", 0, 0, false, false)
  if err != nil { t.Fatal(err) }

  newval, err := testcluster.clients[0].Incr(key)
  if err != nil { t.Fatal(err) }
  if newval != 11 { t.Fatalf("Expected '11' for %s, got %d", key, newval) }

  strval, err := testcluster.clients[0].Get(key)
  if err != nil { t.Fatal(err) }
  if string(strval) != "11" {
    t.Fatalf("Expected '11' for %s, got %s", key, strval)
  }
}


func TestDecr(t *testing.T) {
  setupTest()
  key := "test_dec_key1"

  err := testcluster.clients[0].Set(key, "42", 0, 0, false, false)
  if err != nil { t.Fatal(err) }

  newval, err := testcluster.clients[0].Decr(key)
  if err != nil { t.Fatal(err) }

  if newval != 41 { t.Fatalf("Expected 41 for %s, got %d", key, newval) }

  strval, err := testcluster.clients[0].Get(key)
  if err != nil { t.Fatal(err) }
  if string(strval) != "41" {
    t.Fatalf("Expected '41' for %s, got %s", key, strval)
  }
}

func TestIncrby(t *testing.T) {
  setupTest()
  key := "test_incrby_key1"

  err := testcluster.clients[0].Set(key, "111", 0, 0, false, false)
  if err != nil { t.Fatal(err) }

  newval, err := testcluster.clients[0].IncrBy(key, 89)
  if err != nil { t.Fatal(err) }

  if newval != 200 { t.Fatalf("Expected 200 for %s, got %d", key, newval) }

  strval, err := testcluster.clients[0].Get(key)
  if err != nil { t.Fatal(err) }

  if string(strval) != "200" {
    t.Fatalf("Expected '200' for %s, got %s", key, strval)
  }
}

func TestDecrby(t *testing.T) {
  setupTest()
  key := "test_decrby_key1"

  err := testcluster.clients[0].Set(key, "88", 0, 0, false, false)
  if err != nil { t.Fatal(err) }

  newval, err := testcluster.clients[0].DecrBy(key, 88)
  if err != nil { t.Fatal(err) }

  if newval != 0 { t.Fatalf("Expected 0 for %s, got %d", key, newval) }


  strval, err := testcluster.clients[0].Get(key)
  if err != nil { t.Fatal(err) }

  if string(strval) != "0" {
    t.Fatalf("Expected '0' for %s, got %s", key, strval)
  }
}
