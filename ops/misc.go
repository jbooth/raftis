package ops

import (
	"bytes"
	"errors"
	"fmt"
	mdb "github.com/jbooth/gomdb"
	redis "github.com/jbooth/raftis/redis"
	"strings"
)

type EvalCommand func(args [][]byte, txn *mdb.Txn) ([]byte, error)

var supportedCommands = map[string]EvalCommand{
	"LPOPRANGE": LPOPRANGE,
}

func EVAL(args [][]byte, txn *mdb.Txn) ([]byte, error) {

	println("EVAL", string(bytes.Join(args, []byte(" "))))

	name := strings.ToUpper(string(args[0]))
	command, ok := supportedCommands[name]
	if !ok {
		return nil, redis.NewError(fmt.Sprintf("%s is not supported!", name))
	}
	// second argument is number of keys, we ignore it
	return command(args[2:], txn)
}

func wrongArgsNumberError(command string) error {
	return errors.New(fmt.Sprintf("ERR wrong number of arguments for '%s' command", command))
}

func checkExactArgs(args [][]byte, expected int, command string) error {
	if len(args) != expected {
		return wrongArgsNumberError(command)
	}
	return nil
}

func checkAtLeastArgs(args [][]byte, atLeast int, command string) error {
	if len(args) < atLeast {
		return wrongArgsNumberError(command)
	}
	return nil
}

func checkOddArgs(args [][]byte, atLeast int, command string) error {
	if len(args) < atLeast || len(args)%2 != 1 {
		return wrongArgsNumberError(command)
	}
	return nil
}
