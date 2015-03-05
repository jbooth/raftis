package ops

import (
	mdb "github.com/jbooth/gomdb"
	redis "github.com/jbooth/raftis/redis"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	"io"
)

// 1 arg, key
func GET(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	key := args[0]
	val, err := dbwrap.GetString(txn, key)
	if err == mdb.NotFound {
		// Not found is nil in redis
		return redis.NilReply.WriteTo(w)
	} else if err != nil {
		// write error
		return redis.NewError(err.Error()).WriteTo(w)
	}
	resp := &redis.BulkReply{val}
	return resp.WriteTo(w)
}

// 1 arg, key
func STRLEN(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
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
