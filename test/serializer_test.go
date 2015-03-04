package raftis

import (
	"testing"
	"reflect"
	dbwrap "github.com/jbooth/raftis/dbwrap"
)


func TestSerializer(t *testing.T) {
	inVal := make([][]byte, 0)
	outVal := dbwrap.RawArrayToMembers(dbwrap.BuildRawArray(inVal))

	if !reflect.DeepEqual(inVal, outVal) {
		t.Fatalf("in members %s does not match out members %s", inVal, outVal)
	}

	inVal = append(inVal, []byte("test"))
	outVal = dbwrap.RawArrayToMembers(dbwrap.BuildRawArray(inVal))
	if !reflect.DeepEqual(inVal, outVal) {
		t.Fatalf("in members %s does not match out members %s", inVal, outVal)
	}

	inVal = append(inVal, []byte("test2"))
	outVal = dbwrap.RawArrayToMembers(dbwrap.BuildRawArray(inVal))
	if !reflect.DeepEqual(inVal, outVal) {
		t.Fatalf("in members %s does not match out members %s", inVal, outVal)
	}

	outVal = dbwrap.MapToMembers(dbwrap.MembersToMap(inVal))
	if !reflect.DeepEqual(inVal, outVal) {
		t.Fatalf("in members %s does not match out members %s", inVal, outVal)
	}

}