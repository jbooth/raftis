package ops

import (
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
	"io"
)

// args: key
func SCARD(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	if len(args) != 1 {
		return redis.NewError("ERR wrong number of arguments for 'scard' command").WriteTo(w)
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
