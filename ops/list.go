package ops

import (
	binary "encoding/binary"
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
)

//
// Members 		[][]byte
// MembersList 	[]byte  = len(m1) + m1 ... len(mn) + mn
// RawList 		[]byte = len(n) + MembersList(n)
// RawListValue []byte = ttl + LIST type + RawList
//
// todo: add structs to be more explicit about types above

func MembersToMembersList(members [][]byte) []byte {
	return AppendMembersToMembersList(nil, members)
}

func MembersListToMembers(n int, membersList []byte) [][]byte {
	members := make([][]byte, n)
	for i := 0; i < n; i++ {
		l, rest := ExtractLength(membersList)
		members[i] = rest[:l]
		membersList = rest[l:]
	}
	return members
}

func AppendMembersToMembersList(membersList []byte, members [][]byte) []byte {
	for _, member := range members {
		membersList = append(membersList, withLength(member)...)
	}
	return membersList
}

func BuildRawListValue(expiration uint32, length uint32, membersList []byte) []byte {
	return dbwrap.BuildRawValue(expiration, dbwrap.LIST, BuildRawList(length, membersList))
}

func BuildRawList(length uint32, membersList []byte) []byte {
	return prependLength(length, membersList)
}

// prepends length of val ot val
func withLength(val []byte) []byte {
	return prependLength(uint32(len(val)), val)
}

//prepends 4 bytes with l in them to prependTo
func prependLength(l uint32, prependTo []byte) []byte {
	return append(lengthInBytes(l), prependTo...)
}

// takes first 4 bytes
func ExtractLength(withLength []byte) (uint32, []byte) {
	length := binary.LittleEndian.Uint32(withLength[:4])
	return length, withLength[4:]
}

// converts uint to 4 bytes
func lengthInBytes(l uint32) []byte {
	lengthSpace := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthSpace, uint32(l))
	return lengthSpace
}

func RPUSH(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	//todo: check args ,if not enough return "ERR wrong number of arguments for 'rpush' command"
	key := args[0]
	newMembers := args[1:]

	newMembersLength := uint32(len(newMembers))
	var listValue []byte
	var newLength uint32
	dbi, expiration, rawList, err := dbwrap.GetRawListForWrite(txn, key)
	if err == mdb.NotFound {

		membersList := MembersToMembersList(newMembers)
		// must be -1 instead of 0, uint32 is the cause
		newLength = newMembersLength
		listValue = BuildRawListValue(0, newMembersLength, membersList)
	} else if err != nil {
		return redis.WrapStatus(err.Error()), nil
	} else {
		currentLength, currentMembersList := ExtractLength(rawList)
		newLength = currentLength + newMembersLength
		listValue = BuildRawListValue(expiration, newLength,
			AppendMembersToMembersList(currentMembersList, newMembers))
	}
	err = txn.Put(dbi, key, listValue, 0)
	if err != nil {
		return redis.WrapStatus(err.Error()), nil
	}
	return redis.WrapInt(int(newLength)), txn.Commit()
}
