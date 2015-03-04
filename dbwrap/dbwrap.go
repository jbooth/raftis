package dbwrap

import (
	"encoding/binary"
	"errors"
	"time"
	mdb "github.com/jbooth/gomdb"
)

const (
	STRING = iota
	LIST
	SET
	HASH
	SORTEDSET
)

// parse
func ParseRawValue(rawVal []byte) (uint32, uint8, []byte) {
	expiration := binary.LittleEndian.Uint32(rawVal[0:4])
	type_ := uint8(rawVal[4])
	val := rawVal[5:]
	return expiration, type_, val
}

func ParseString(rawVal []byte) (uint32, []byte, error) {
	expiration, type_, val := ParseRawValue(rawVal)
	if type_ != STRING {
		return 0, nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return expiration, val, nil
}

func ParseHash(rawVal []byte) (uint32, [][]byte, error) {
	expiration, type_, val :=  ParseRawValue(rawVal)
	if type_ != HASH {
		return 0, nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return expiration, RawArrayToMembers(val), nil
}

func ParseList(rawVal []byte) (uint32, []byte, error) {
	expiration, type_, val := ParseRawValue([]byte(rawVal))
	if type_ != LIST {
		return 0, nil, errors.New("type mismatch")
	}
	return expiration, val, nil
}

// build
func BuildRawValue(expiration uint32, type_ uint8, val []byte) []byte {
	rawVal := append(make([]byte, 4), byte(type_))
	rawVal = append(rawVal, val...)
	binary.LittleEndian.PutUint32(rawVal[0:4], expiration)
	return rawVal
}

func BuildString(expiration uint32, val []byte) []byte {
	return BuildRawValue(expiration, STRING, val)
}

func BuildList(expiration uint32, val [][]byte) []byte {
	return BuildRawValue(expiration, LIST, BuildRawArray(val))
}

func BuildSet(expiration uint32, val [][]byte) []byte {
	return BuildRawValue(expiration, SET, BuildRawArray(val))
}

func BuildHash(expiration uint32, val [][]byte) []byte {
	return BuildRawValue(expiration, HASH, BuildRawArray(val))
}

// ttls
const epoch int64 = 1425410200

func GetNow() uint32 {
	return uint32(time.Now().Unix() - epoch)
}

// convenience
func GetDBI(txn *mdb.Txn, dbiFlags uint) (mdb.DBI, error) {
	table := "onlyTable"
	return txn.DBIOpen(&table, dbiFlags)
}

func GetBytes(txn *mdb.Txn, key []byte, dbiFlags uint) (mdb.DBI, []byte, error) {
	dbi, err := GetDBI(txn, dbiFlags)
	if err != nil {
		return dbi, nil, err
	}
	rawVal, err := txn.Get(dbi, key)
	if err != nil {
		return dbi, nil, err
	}
	return dbi, rawVal, nil
}

func GetRawValueForWrite(txn *mdb.Txn, key []byte) (mdb.DBI, uint32, uint8, []byte, error) {
	dbi, rawVal, err := GetBytes(txn, key, mdb.CREATE)
	if err != nil {
		return dbi, 0, 0, nil, err
	}
	expiration, type_, val := ParseRawValue(rawVal)
	if Expired(expiration) {
		return dbi, 0, 0, nil, mdb.NotFound
	}
	return dbi, expiration, type_, val, nil
}

func GetString(txn *mdb.Txn, key []byte) ([]byte, error) {
	_, rawVal, err := GetBytes(txn, key, 0)
	if err != nil {
		return nil, err
	}
	expiration, val, err := ParseString(rawVal)
	if err != nil {
		return nil, err
	}
	if Expired(expiration) {
		return nil, mdb.NotFound
	}
	return val, nil
}

func GetStringForWrite(txn *mdb.Txn, key []byte) (mdb.DBI, uint32, []byte, error) {
	dbi, rawVal, err := GetBytes(txn, key, mdb.CREATE)
	if err != nil {
		return dbi, 0, nil, err
	}
	expiration, val, err := ParseString(rawVal)
	if err != nil {
		return dbi, 0, nil, err
	}
	if Expired(expiration) {
		return dbi, 0, nil, mdb.NotFound
	}
	return dbi, expiration, val, nil
}

func GetHash(txn *mdb.Txn, key []byte) ([][]byte, error) {
	_, rawVal, err := GetBytes(txn, key, 0)
	if err != nil {
		return nil, err
	}
	expiration, val, err := ParseHash(rawVal)
	if err != nil {
		return nil, err
	}
	if Expired(expiration) {
		return nil, mdb.NotFound
	}
	return val, nil
}

func GetHashForWrite(txn *mdb.Txn, key []byte) (mdb.DBI, uint32, [][]byte, error) {
	dbi, rawVal, err := GetBytes(txn, key, mdb.CREATE)
	if err != nil {
		return dbi, 0, nil, err
	}
	expiration, val, err := ParseHash(rawVal)
	if err != nil {
		return dbi, 0, nil, err
	}
	if Expired(expiration) {
		return dbi, 0, nil, mdb.NotFound
	}
	return dbi, expiration, val, nil
}

func GetRawList(txn *mdb.Txn, key []byte) ([]byte, error) {
	_, rawVal, err := GetBytes(txn, key, 0)
	if err != nil {
		return nil, err
	}
	expiration, val, err := ParseList(rawVal)
	if err != nil {
		return nil, err
	}
	if Expired(expiration) {
		return nil, mdb.NotFound
	}
	return val, nil
}

func GetRawListForWrite(txn *mdb.Txn, key []byte) (mdb.DBI, uint32, []byte, error) {
	dbi, rawVal, err := GetBytes(txn, key, mdb.CREATE)
	if err != nil {
		return dbi, 0, nil, err
	}
	expiration, val, err := ParseList(rawVal)
	if err != nil {
		return dbi, 0, nil, err
	}
	if Expired(expiration) {
		return dbi, 0, nil, mdb.NotFound
	}
	return dbi, expiration, val, nil
}

func Expired(expiration uint32) bool {
	return expiration != 0 && expiration < GetNow()
}
