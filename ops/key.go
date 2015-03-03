package ops

import (
	mdb "github.com/jbooth/gomdb"
	redis "github.com/jbooth/raftis/redis"
)

func DEL(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	table := "onlyTable"
	// there must be a better way to print a slice :)
	print("DEL")
	for _, k := range args {
		print(" ", string(k))
	}
	println("")

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
