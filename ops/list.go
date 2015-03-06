package ops

import (
	"bytes"
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
)

func BuildList(expiration uint32, length uint32, membersList []byte) []byte {
	return dbwrap.BuildRawValue(expiration, dbwrap.LIST, dbwrap.BuildRawArray0(length, membersList))
}

func RPUSH(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	//todo: check args ,if not enough return "ERR wrong number of arguments for 'rpush' command"
	key := args[0]
	newMembers := args[1:]
	println("RPUSH", string(key), string(bytes.Join(newMembers, []byte(" "))))

	newMembersLength := uint32(len(newMembers))
	var listValue []byte
	var newLength uint32
	dbi, expiration, rawList, err := dbwrap.GetRawListForWrite(txn, key)
	if err == mdb.NotFound {
		newLength = newMembersLength
		listValue = dbwrap.BuildList(0, newMembers)
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	} else {
		currentLength, currentMembersList := dbwrap.ExtractLength(rawList)
		newLength = currentLength + newMembersLength
		listValue = BuildList(expiration, newLength,
			dbwrap.AppendMembersToMembersArray(currentMembersList, newMembers))
	}
	err = txn.Put(dbi, key, listValue, 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapInt(int(newLength)), txn.Commit()
}

//=============================================================
//custom commands supported via EVAL

func LPOPRANGE(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	//todo: check args ,if not enough return "ERR wrong number of arguments for 'rpush' command"

	println("LPOPRANGE", string(bytes.Join(args, []byte(" "))))
	key := args[0]
	start, err := toIntArg(args[1])
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	end, err := toIntArg(args[2])
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}

	if start > end || start < 0 {
		return redis.WrapArray(nil), txn.Commit()
	}

	dbi, expiration, rawList, err := dbwrap.GetRawListForWrite(txn, key)
	if err == mdb.NotFound {
		return redis.WrapArray(nil), txn.Commit()
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	} else {
		length, membersList := dbwrap.ExtractLength(rawList)
		members := dbwrap.MembersArrayToMembers(length, membersList)
		if end > int(length) {
			end = int(length)
		} else {
			end++
		}
		membersRange := make([][]byte, end-start)
		copy(membersRange, members[start:end])

		newMembers := append(members[:start], members[end:]...)

		listValue := dbwrap.BuildList(expiration, newMembers)

		err = txn.Put(dbi, key, listValue, 0)

		if err != nil {
			return redis.WrapStatus(err.Error()), nil
		}
		return redis.WrapArray(membersRange), txn.Commit()
	}
}
