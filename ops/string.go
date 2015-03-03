package ops

import (
	"fmt"
  "strconv"
	mdb "github.com/jbooth/gomdb"
	redis "github.com/jbooth/raftis/redis"
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
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapStatus("OK"), txn.Commit()
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
		return redis.WrapStatus(err.Error()), nil
	}
	err = txn.Put(dbi, key, newVal, 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapString(oldVal), txn.Commit()
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
	appendVal := args[1]
	table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
	if err != nil {


		return nil, err
	}
	var val []byte
	val, err = txn.Get(dbi, key)
	if err == mdb.NotFound {
		val = appendVal
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	} else {
		val = append(val, appendVal...)
	}

	err = txn.Put(dbi, key, val, 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapInt(len(val)), txn.Commit() //success
}


func Counter(key []byte, increment int, txn *mdb.Txn) ([]byte, error) {
  table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)
  if err != nil { return redis.WrapStatus(err.Error()), nil }

  current_value, err := txn.Get(dbi, key)
  if err != nil { return redis.WrapStatus(err.Error()), nil }

  current_value_int, err := strconv.Atoi(string(current_value[:]))
  if err != nil { return redis.WrapStatus(err.Error()), nil }

  new_value_int := current_value_int + increment
  new_value := strconv.Itoa(new_value_int)
	err = txn.Put(dbi, key, []byte(new_value), 0)
	if err != nil { return redis.WrapStatus(err.Error()), nil }

  return redis.WrapInt(new_value_int), txn.Commit()
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

