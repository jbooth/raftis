package utils

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

func ParseString(rawVal []byte) (uint32, string, error) {
	expiration, type_, val :=  ParseRawValue([]byte(rawVal))
	if type_ != STRING {
		return 0, "", errors.New("type mismatch")
	}
	return expiration, string(val), nil
}

// build
func BuildRawValue(expiration uint32, type_ uint8, val []byte) ([] byte) {
	rawVal := append(make([]byte, 4), byte(type_))
	rawVal = append(rawVal, val...)
	binary.LittleEndian.PutUint32(rawVal[0:4], expiration)
	return rawVal
}

func BuildString(expiration uint32, val string) ([] byte) {
	return BuildRawValue(expiration, STRING, []byte(val))
}

// ttls
const epoch int64 = 1425410200
func GetNow() (uint32) {
	return uint32(time.Now().Unix()-epoch)
}

// convenience
func getBytes(txn *mdb.Txn, key []byte, dbiFlags uint) (mdb.DBI, []byte, error) {
	table := "onlyTable"
	dbi, err := txn.DBIOpen(&table, dbiFlags)
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
	dbi, rawVal, err := getBytes(txn, key, mdb.CREATE)
	if err != nil {
		return dbi, 0, 0, nil, err
	}
	expiration, type_, val := ParseRawValue(rawVal)
	if Expired(expiration) {
		return dbi, 0, 0, nil, mdb.NotFound
	}
	return dbi, expiration, type_, val, nil
}

func GetString(txn *mdb.Txn, key []byte) (string, error) {
	_, rawVal, err := getBytes(txn, key, 0)
	if err != nil {
		return "", err
	}
	expiration, val, err := ParseString(rawVal)
	if err != nil {
		return "", err
	}
	if Expired(expiration) {
		return "", mdb.NotFound
	}
	return val, nil
}

func GetStringForWrite(txn *mdb.Txn, key []byte) (mdb.DBI, uint32, string, error) {
	dbi, rawVal, err := getBytes(txn, key, mdb.CREATE)
	if err != nil {
		return dbi, 0, "", err
	}
	expiration, val, err := ParseString(rawVal)
	if err != nil {
		return dbi, 0, "", err
	}
	if Expired(expiration) {
		return dbi, 0, "", mdb.NotFound
	}
	return dbi, expiration, val, nil
}

func Expired(expiration uint32) (bool) {
	return expiration != 0 && expiration < GetNow()
}