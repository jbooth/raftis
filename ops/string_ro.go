package ops

import (
	mdb "github.com/jbooth/gomdb"
	redis "github.com/jbooth/raftis/redis"
	"io"
)

// 1 arg, key
func GET(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	key := args[0]
	table := "onlyTable"
	println("GET " + string(key))
	dbi, err := txn.DBIOpen(&table, 0)
	if err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	val, err := txn.GetVal(dbi, key)
	var resp redis.ReplyWriter

	if err == mdb.NotFound {
		// Not found is nil in redis
		resp = redis.NilReply
	} else if err != nil {
		// write error
		resp = redis.NewError(err.Error())
	} else {
		resp = &redis.BulkReply{val.RawBytes()}
	}
	// write result
	return resp.WriteTo(w)
}
