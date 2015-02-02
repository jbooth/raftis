package ops

import (
	mdb "github.com/jbooth/gomdb"
	"io"
)

// 1 arg, key
func GET(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	key := args[0]
	table := "onlyTable"
	println("GET " + string(key))
	dbi, err := txn.DBIOpen(&table, 0)
	if err != nil {
		return ErrorReply{err}.WriteTo(w)
	}
	val, err := txn.GetVal(dbi, key)
	if err == mdb.NotFound {
		// Not found is nil in redis
		return NilReply().WriteTo(w)
	} else if err != nil {
		// write error
		return ErrorReply{err}.WriteTo(w)
	}
	// write result
	return StringReply{val.RawBytes()}.WriteTo(w)
}
