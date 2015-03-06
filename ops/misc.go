package ops

import (
	"bytes"
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
