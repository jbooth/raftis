package ops

import (
	//	"fmt"
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
	"strconv"
)

// args are key, val
func SET(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkExactArgs(args, 2, "set"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	key := args[0]
	val := args[1]
	dbi, err := dbwrap.GetDBI(txn, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	err = txn.Put(dbi, key, dbwrap.BuildString(0, val), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapStatus("OK"), txn.Commit()
}

// args are key, newVal, returns oldVal
func GETSET(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkExactArgs(args, 2, "getset"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	key := args[0]
	newVal := args[1]
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
	if err := checkExactArgs(args, 2, "setnx"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	key := args[0]
	newVal := args[1]
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
	if err := checkExactArgs(args, 2, "append"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	key := args[0]
	appendVal := args[1]
	dbi, exp, oldVal, err := dbwrap.GetStringForWrite(txn, key)
	var newVal []byte
	if err == mdb.NotFound {
		newVal = appendVal
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	} else {
		newVal = append(oldVal, appendVal...)
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
		currentValue = []byte("0")
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	currentValueInt, err := strconv.Atoi(string(currentValue[:]))
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	newValueInt := currentValueInt + increment
	newValue := []byte(strconv.Itoa(newValueInt))
	err = txn.Put(dbi, key, dbwrap.BuildString(exp, newValue), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	return redis.WrapInt(newValueInt), txn.Commit()
}

// args: key
func INCR(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkExactArgs(args, 1, "incr"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	return Counter(args[0], 1, txn)
}

// agrs: key
func DECR(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkExactArgs(args, 1, "decr"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	return Counter(args[0], -1, txn)
}

// args: key delta
func INCRBY(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkExactArgs(args, 2, "incrby"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	increment, err := strconv.Atoi(string(args[1][:]))
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	return Counter(args[0], increment, txn)
}

// args: key delta
func DECRBY(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkExactArgs(args, 2, "decrby"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	increment, err := strconv.Atoi(string(args[1][:]))
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	return Counter(args[0], -increment, txn)
}
