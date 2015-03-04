package ops

import (
	"fmt"
	"strconv"
	mdb "github.com/jbooth/gomdb"
	redis "github.com/jbooth/raftis/redis"
	dbwrap "github.com/jbooth/raftis/dbwrap"
)

// args are key, val
func SET(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
	val := string(args[1])
	table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	fmt.Printf("SET %s %s \n", string(key), val)
	err = txn.Put(dbi, key, dbwrap.BuildString(0, val), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapStatus("OK"), txn.Commit()
}

// args are key, newVal, returns oldVal
func GETSET(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
	newVal := string(args[1])
	dbi, _, oldVal, err := dbwrap.GetStringForWrite(txn, key)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	err = txn.Put(dbi, key, dbwrap.BuildString(0, newVal), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapString(oldVal), txn.Commit()
}

// args are key, val
func SETNX(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
	newVal := string(args[1])
	dbi, _, _, err := dbwrap.GetStringForWrite(txn, key)
	if err == mdb.NotFound {
		err = txn.Put(dbi, key, dbwrap.BuildString(0, newVal), 0)
		if err != nil {
			return redis.WrapStatus(err.Error()), nil
		}
		return redis.WrapInt(1), txn.Commit() //success
	}
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapInt(0), nil // had key already
}

// args are key, val
// return value is int of new val length
func APPEND(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
	appendVal := string(args[1])
	dbi, exp, oldVal, err := dbwrap.GetStringForWrite(txn, key)
	var newVal string
	if err == mdb.NotFound {
		newVal = appendVal
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	} else {
		newVal = oldVal + appendVal
	}
	err = txn.Put(dbi, key, dbwrap.BuildString(exp, newVal), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapInt(len(newVal)), txn.Commit() //success
}


func Counter(key []byte, increment int, txn *mdb.Txn) ([]byte, error) {
	dbi, exp, currentValue, err := dbwrap.GetStringForWrite(txn, key)
	if err == mdb.NotFound {
		currentValue = "0"
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	currentValueInt, err := strconv.Atoi(string(currentValue[:]))
	if err != nil { return redis.WrapStatus(err.Error()), nil }

	newValueInt := currentValueInt + increment
	newValue := strconv.Itoa(newValueInt)
	err = txn.Put(dbi, key, dbwrap.BuildString(exp, newValue), 0)
	if err != nil { return redis.WrapStatus(err.Error()), nil }

	return redis.WrapInt(newValueInt), txn.Commit()
}

func INCR(args [][]byte, txn *mdb.Txn) ([]byte, error) {
  return Counter(args[0], 1, txn)
}

func DECR(args [][]byte, txn *mdb.Txn) ([]byte, error) {
  return Counter(args[0], -1, txn)
}

func INCRBY(args [][]byte, txn *mdb.Txn) ([]byte, error) {
  increment, err := strconv.Atoi(string(args[1][:]))
  if err != nil { return redis.WrapStatus(err.Error()), nil }

  return Counter(args[0], increment, txn)
}

func DECRBY(args [][]byte, txn *mdb.Txn) ([]byte, error) {
  increment, err := strconv.Atoi(string(args[1][:]))
  if err != nil { return redis.WrapStatus(err.Error()), nil }

  return Counter(args[0], -increment, txn)
}
