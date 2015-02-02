package ops

import (
	"fmt"
	mdb "github.com/jbooth/gomdb"
)

// args are key, val
func SET(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
	val := args[1]
	table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	fmt.Printf("SET %s %s \n", string(key), string(val))
	err = txn.Put(dbi, key, val, 0)
	if err != nil {
		return WrapStatus(err.Error()), nil
	}
	return WrapStatus("OK"), txn.Commit()
}

// args are key, newVal, returns oldVal
func GETSET(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
	newVal := args[1]
	table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	oldVal, err := txn.Get(dbi, key)
	if err != nil {
		return WrapStatus(err.Error()), nil
	}
	err = txn.Put(dbi, key, newVal, 0)
	if err != nil {
		return WrapStatus(err.Error()), nil
	}
	return WrapString(oldVal), txn.Commit()
}

// args are key, val
func SETNX(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
	newVal := args[1]
	table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	_, err = txn.Get(dbi, key)
	if err == mdb.NotFound {
		err = txn.Put(dbi, key, newVal, 0)
		if err != nil {
			return WrapStatus(err.Error()), nil
		}
		return WrapInt(1), txn.Commit() //success
	}
	if err != nil {
		return WrapStatus(err.Error()), nil
	}
	return WrapInt(0), nil // had key already
}
