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


func INCR(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
  table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)

  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  oldVal, err := txn.Get(dbi, key)
  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  oldValInt, err := strconv.Atoi(string(oldVal[:]))
  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  newVal := oldValInt + 1
  newValStr := strconv.Itoa(newVal)
	err = txn.Put(dbi, key, []byte(newValStr), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}


  return redis.WrapInt(newVal), txn.Commit()
}


func DECR(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
  table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)

  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  oldVal, err := txn.Get(dbi, key)
  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  oldValInt, err := strconv.Atoi(string(oldVal[:]))
  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  newVal := oldValInt - 1
  newValStr := strconv.Itoa(newVal)
	err = txn.Put(dbi, key, []byte(newValStr), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

  return redis.WrapInt(newVal), txn.Commit()
}


func INCRBY(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
  incr_raw :=  args[1]

  incr, err := strconv.Atoi(string(incr_raw[:]))
  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)

  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  oldVal, err := txn.Get(dbi, key)
  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  oldValInt, err := strconv.Atoi(string(oldVal[:]))
  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  newVal := oldValInt + incr
  newValStr := strconv.Itoa(newVal)
	err = txn.Put(dbi, key, []byte(newValStr), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

  return redis.WrapInt(newVal), txn.Commit()
}


func DECRBY(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	key := args[0]
  step_raw :=  args[1]

  step, err := strconv.Atoi(string(step_raw[:]))
  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, mdb.CREATE)

  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  oldVal, err := txn.Get(dbi, key)
  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  oldValInt, err := strconv.Atoi(string(oldVal[:]))
  if err != nil {
    return redis.WrapStatus(err.Error()), nil
  }

  newVal := oldValInt - step
  newValStr := strconv.Itoa(newVal)
	err = txn.Put(dbi, key, []byte(newValStr), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

  return redis.WrapInt(newVal), txn.Commit()
}

