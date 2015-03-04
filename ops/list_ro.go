package ops

import (
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
	"io"
)

func LLEN(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	//todo: check args ,if not enough return "ERR wrong number of arguments for 'rpush' command"

	key := args[0]
	rawList, err := dbwrap.GetRawList(txn, key)
	var resp redis.ReplyWriter
	if err == mdb.NotFound {
		// Redis returns 0 for non-existing key
		resp = &redis.IntegerReply{0}
	} else if err != nil {
		// write error
		resp = redis.NewError(err.Error())
	} else {
		length, _ := ExtractLength(rawList)
		// Redis returns 1 for existing key
		resp = &redis.IntegerReply{int(length)}
	}
	// write result
	return resp.WriteTo(w)

}
