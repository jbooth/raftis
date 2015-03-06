package ops

import (
	"bytes"
	mdb "github.com/jbooth/gomdb"
	redis "github.com/jbooth/raftis/redis"
)

// args: key1, [key2 ...]
func DEL(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkAtLeastArgs(args, 1, "del"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	table := "onlyTable"
	println("DEL ", bytes.Join(args, []byte(" ")))

	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	deleted := 0
	for _, key := range args {
		_, err := txn.Get(dbi, key)
		if err == nil {
			err := txn.Del(dbi, key, nil)
			deleted++
			if err != nil {
				return redis.WrapStatus(err.Error()), nil
			}
		} else if err != mdb.NotFound {
			return redis.WrapStatus(err.Error()), nil
		}
	}
	return redis.WrapInt(deleted), txn.Commit()
}
