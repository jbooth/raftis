package ops

import (
	mdb "github.com/jbooth/gomdb"
	redis "github.com/jbooth/raftis/redis"
	"io"
)

func EXISTS(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	key := args[0]
	table := "onlyTable"
	println("EXISTS " + string(key))
	dbi, err := txn.DBIOpen(&table, 0)
	if err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	_, err = txn.Get(dbi, key)
	var resp redis.ReplyWriter
	if err == mdb.NotFound {
		// Redis returns 0 for non-existing key
		resp = &redis.IntegerReply{0}
	} else if err != nil {
		// write error
		resp = redis.NewError(err.Error())
	} else {
		// Redis returns 1 for existing key
		resp = &redis.IntegerReply{1}
	}
	// write result
	return resp.WriteTo(w)
}
