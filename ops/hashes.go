package ops

import (
	"bytes"
	"fmt"
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
	"io"
	"strconv"
)

// READS
// args: key field
func HGET(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	if err := checkExactArgs(args, 2, "hget"); err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}

	key := args[0]
	field := args[1]
	println("HGET " + string(key) + " " + string(field))
	val, err := dbwrap.GetHash(txn, key)
	if err == mdb.NotFound {
		val = make([][]byte, 0)
	} else if err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	var fieldValue []byte = nil
	for i := 0; i < len(val); i += 2 {
		if bytes.Equal(field, val[i]) {
			fieldValue = val[i+1]
			break
		}
	}
	resp := &redis.BulkReply{fieldValue}
	return resp.WriteTo(w)
}

// args: key field [field ...]
func HMGET(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	if err := checkAtLeastArgs(args, 2, "hmget"); err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	key := args[0]
	fields := args[1:]
	fmt.Printf("HMGET %s %s \n", string(key), fields)
	val, err := dbwrap.GetHash(txn, key)
	if err == mdb.NotFound {
		val = make([][]byte, 0)
	} else if err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	mapVal := dbwrap.MembersToMap(val)
	ret := make([][]byte, 0)
	var fieldValue string
	var exists bool
	for _, f := range fields {
		if fieldValue, exists = mapVal[string(f)]; !exists {
			ret = append(ret, nil)
		} else {
			ret = append(ret, []byte(fieldValue))
		}
	}
	resp := &redis.ArrayReply{ret}
	return resp.WriteTo(w)
}

// args: key
func HGETALL(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	if err := checkExactArgs(args, 1, "hgetall"); err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	key := args[0]
	println("HGETALL " + string(key))
	val, err := dbwrap.GetHash(txn, key)
	if err == mdb.NotFound {
		val = make([][]byte, 0)
	} else if err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	resp := &redis.ArrayReply{val}
	return resp.WriteTo(w)
}

// WRITES
// args: key field value
func HSET(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkExactArgs(args, 3, "hset"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	key := args[0]
	field := string(args[1])
	value := string(args[2])
	fmt.Printf("HSET %s %s %s \n", string(key), field, value)

	dbi, exp, val, err := dbwrap.GetHashForWrite(txn, key)
	if err == mdb.NotFound {
		val = make([][]byte, 0)
		exp = 0
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	mapVal := dbwrap.MembersToMap(val)
	ret := 1
	if _, exists := mapVal[field]; exists {
		ret = 0
	}
	mapVal[field] = value
	newVal := dbwrap.MapToMembers(mapVal)

	err = txn.Put(dbi, key, dbwrap.BuildHash(exp, newVal), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapInt(ret), txn.Commit()
}

// args: key field value [field value ...]
func HMSET(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkOddArgs(args, 3, "hmset"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	key := args[0]
	newFields := args[1:]
	fmt.Printf("HMSET %s %s \n", string(key), newFields)

	dbi, exp, val, err := dbwrap.GetHashForWrite(txn, key)
	if err == mdb.NotFound {
		val = make([][]byte, 0)
		exp = 0
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	mapVal := dbwrap.MembersToMap(val)
	for i := 0; i < len(newFields); i += 2 {
		mapVal[string(newFields[i])] = string(newFields[i+1])
	}
	newVal := dbwrap.MapToMembers(mapVal)

	err = txn.Put(dbi, key, dbwrap.BuildHash(exp, newVal), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapStatus("OK"), txn.Commit()
}

// args: key field increment
func HINCRBY(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkExactArgs(args, 3, "hincrby"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	key := args[0]
	field := string(args[1])
	increment, err := strconv.Atoi(string(args[2]))

	fmt.Printf("HINCRBY %s %s %s \n", string(key), field, increment)

	dbi, exp, val, err := dbwrap.GetHashForWrite(txn, key)
	if err == mdb.NotFound {
		val = make([][]byte, 0)
		exp = 0
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	mapVal := dbwrap.MembersToMap(val)
	var currentValue string
	var exists bool
	if currentValue, exists = mapVal[field]; !exists {
		currentValue = "0"
	}

	currentValueInt, err := strconv.Atoi(currentValue)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	newValueInt := currentValueInt + increment
	mapVal[field] = strconv.Itoa(newValueInt)

	newVal := dbwrap.MapToMembers(mapVal)
	err = txn.Put(dbi, key, dbwrap.BuildHash(exp, newVal), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapInt(newValueInt), txn.Commit()
}

// args: key field [field ...]
func HDEL(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkAtLeastArgs(args, 2, "hdel"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	key := args[0]
	fields := args[1:]
	fmt.Printf("HDEL %s %s \n", string(key), fields)

	dbi, exp, val, err := dbwrap.GetHashForWrite(txn, key)
	if err == mdb.NotFound {
		val = make([][]byte, 0)
		exp = 0
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	mapVal := dbwrap.MembersToMap(val)
	deleted := 0
	for i := 0; i < len(fields); i++ {
		f := string(fields[i])
		if _, exists := mapVal[f]; exists {
			delete(mapVal, f)
			deleted++
		}
	}
	if deleted > 0 {
		newVal := dbwrap.MapToMembers(mapVal)
		err = txn.Put(dbi, key, dbwrap.BuildHash(exp, newVal), 0)
		if err != nil {
			return redis.WrapStatus(err.Error()), nil
		}
	}
	return redis.WrapInt(deleted), txn.Commit()
}
