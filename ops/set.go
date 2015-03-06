package ops

import (
	"bytes"
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
)

// WRITES
// args: key member1 [member2 ...]
func SADD(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if err := checkAtLeastArgs(args, 2, "sadd"); err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	key := args[0]
	newMembersArray := args[1:]
	println("SADD", string(key), string(bytes.Join(newMembersArray, []byte(" "))))

	dbi, expiration, rawSet, err := dbwrap.GetRawSetForWrite(txn, key)

	var finalMembersSet map[string]struct{}
	var added int

	if err == mdb.NotFound {
		finalMembersSet, added = dbwrap.AddMembersToSet(make(map[string]struct{}), newMembersArray)
		expiration = 0
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	} else {
		currentMembersSet, _ := dbwrap.MembersToSet(dbwrap.RawArrayToMembers(rawSet))
		finalMembersSet, added = dbwrap.AddMembersToSet(currentMembersSet, newMembersArray)
	}
	finalMembersArray := dbwrap.SetToMembers(finalMembersSet)

	err = txn.Put(dbi, key, dbwrap.BuildSet(expiration, finalMembersArray), 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapInt(added), txn.Commit()
}
