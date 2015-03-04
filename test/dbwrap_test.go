package raftis

import (
	"testing"
	dbwrap "github.com/jbooth/raftis/dbwrap"
)


func TestBuildString(t *testing.T) {
	setupTest()

	var inExpiration uint32 = 123
	inString := "test123"

	packed := dbwrap.BuildString(inExpiration, inString)

	outExpiration, outString, _ := dbwrap.ParseString(packed)

	if inExpiration != outExpiration {
		t.Fatalf("in expiration %s does not match out expiration %s", inExpiration, outExpiration)
	}
	if inString != outString {
		t.Fatalf("in string %s does not match out string %s", inString, outString)
	}
}