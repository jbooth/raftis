package raftis

import (
	"testing"
	utils "github.com/jbooth/raftis/utils"
)


func TestBuildString(t *testing.T) {
	setupTest()

	var inExpiration uint32 = 123
	inString := "test123"

	packed := utils.BuildString(inExpiration, inString)

	outExpiration, outString, _ := utils.ParseString(packed)

	if inExpiration != outExpiration {
		t.Fatalf("in expiration %s does not match out expiration %s", inExpiration, outExpiration)
	}
	if inString != outString {
		t.Fatalf("in string %s does not match out string %s", inString, outString)
	}
}