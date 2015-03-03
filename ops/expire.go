package ops

import (
	"strconv"
	mdb "github.com/jbooth/gomdb"
	redis "github.com/jbooth/raftis/redis"
	utils "github.com/jbooth/raftis/utils"
)

// args are key, seconds
func EXPIRE(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
	seconds := string(args[1])
	println("EXPIRE " + string(key) + " " + seconds)

	secondsInt, err := strconv.Atoi(seconds)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	dbi, _, type_, val, err := utils.GetRawValueForWrite(txn, key)
	if err == mdb.NotFound {
		return redis.WrapInt(0), nil
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	exp := utils.GetNow() + uint32(secondsInt)
	err = txn.Put(dbi, key, utils.BuildRawValue(exp, type_, val), 0)
	return redis.WrapInt(1), txn.Commit()
}