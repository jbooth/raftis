package ops

import (
	"fmt"
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
	"io"
)

// args: key
func GET(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	if err := checkExactArgs(args, 1, "get"); err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}

	key := args[0]
	val, err := dbwrap.GetString(txn, key)
	if err == mdb.NotFound {
		// Not found is nil in redis
		return redis.NilReply.WriteTo(w)
	} else if err != nil {
		// write error
		return redis.NewError(err.Error()).WriteTo(w)
	}
	fmt.Printf("GET sending allegedly valid response\n")
	resp := &redis.BulkReply{val}
	return resp.WriteTo(w)
}

// args: key
func STRLEN(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	if err := checkExactArgs(args, 1, "strlen"); err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}

	key := args[0]
	val, err := dbwrap.GetString(txn, key)
	if err == mdb.NotFound {
		resp := &redis.IntegerReply{0}
		return resp.WriteTo(w)
	} else if err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	resp := &redis.IntegerReply{len(val)}
	return resp.WriteTo(w)
}
