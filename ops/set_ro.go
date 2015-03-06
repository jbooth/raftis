package ops

import (
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
	"io"
)

// args: key
func SCARD(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	if err := checkExactArgs(args, 1, "scard"); err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	key := args[0]
	println("SCARD", string(key))
	rawSet, err := dbwrap.GetRawSet(txn, key)
	var resp redis.ReplyWriter
	if err == mdb.NotFound {
		// Redis returns 0 for non-existing key
		resp = &redis.IntegerReply{0}
	} else if err != nil {
		// write error
		resp = redis.NewError(err.Error())
	} else {
		length, _ := dbwrap.ExtractLength(rawSet)
		// Redis returns 1 for existing key
		resp = &redis.IntegerReply{int(length)}
	}
	// write result
	return resp.WriteTo(w)
}

// args: key
func SMEMBERS(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	if err := checkExactArgs(args, 1, "smembers"); err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}

	key := args[0]
	println("SMEMBERS", string(key))

	var resp redis.ReplyWriter

	rawSet, err := dbwrap.GetRawSet(txn, key)
	if err == mdb.NotFound {
		// Redis returns 0 for non-existing key
		return redis.NilArrayReply.WriteTo(w)
	} else if err != nil {
		// write error
		resp = redis.NewError(err.Error())
	} else {
		members := dbwrap.RawArrayToMembers(rawSet)
		resp = &redis.ArrayReply{members}
	}
	// write result
	return resp.WriteTo(w)
}
