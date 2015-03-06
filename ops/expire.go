package ops

import (
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
	"io"
	"strconv"
)

// args are key, seconds
func EXPIRE(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkExactArgs(args, 2, "expire"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	key := args[0]
	seconds := string(args[1])
	println("EXPIRE " + string(key) + " " + seconds)

	secondsInt, err := strconv.Atoi(seconds)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	dbi, _, type_, val, err := dbwrap.GetRawValueForWrite(txn, key)
	if err == mdb.NotFound {
		return redis.WrapInt(0), nil
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	exp := dbwrap.GetNow() + uint32(secondsInt)
	err = txn.Put(dbi, key, dbwrap.BuildRawValue(exp, type_, val), 0)
	return redis.WrapInt(1), txn.Commit()
}

// args: key
func TTL(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	if err := checkExactArgs(args, 1, "ttl"); err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}

	key := args[0]
	println("TTL " + string(key))

	exp, _, _, err := dbwrap.GetRawValue(txn, key)
	ret := -1
	if err == mdb.NotFound {
		ret = -2
	} else if err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	if exp > 0 {
		ret = int(exp - dbwrap.GetNow())
	}
	resp := &redis.IntegerReply{ret}
	return resp.WriteTo(w)
}
