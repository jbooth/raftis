package dbwrap

import (
	"encoding/binary"
)

// Members		[][]byte
// MembersArray		[]byte  = len(m1) + m1 ... len(mn) + mn
// RawArray		[]byte = len(n) + MembersArray(n)
// RawArrayValue	[]byte = ttl + LIST/HASH/SET/SORTEDSET type + RawArray

// SERIALIZER
func MembersToMembersArray(members [][]byte) []byte {
	return AppendMembersToMembersArray(nil, members)
}

func AppendMembersToMembersArray(membersArray []byte, members [][]byte) []byte {
	for _, member := range members {
		membersArray = append(membersArray, withLength(member)...)
	}
	return membersArray
}

func BuildRawArray(members [][]byte) []byte {
	return BuildRawArray0(uint32(len(members)), MembersToMembersArray(members))
}

func BuildRawArray0(length uint32, membersArray []byte) []byte {
	return prependLength(length, membersArray)
}

// prepends length of val to val
func withLength(val []byte) []byte {
	return prependLength(uint32(len(val)), val)
}

//prepends 4 bytes with l in them to prependTo
func prependLength(l uint32, prependTo []byte) []byte {
	return append(lengthInBytes(l), prependTo...)
}

// converts uint to 4 bytes
func lengthInBytes(l uint32) []byte {
	lengthSpace := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthSpace, uint32(l))
	return lengthSpace
}

// DESERIALIZER
// takes first 4 bytes
func ExtractLength(withLength []byte) (uint32, []byte) {
	length := binary.LittleEndian.Uint32(withLength[:4])
	return length, withLength[4:]
}

func RawArrayToMembers(rawArray []byte) [][]byte {
	length, membersArray := ExtractLength(rawArray)
	return MembersArrayToMembers(length, membersArray)
}

func MembersArrayToMembers(n uint32, membersArray []byte) [][]byte {
	members := make([][]byte, n)
	for i := uint32(0); i < n; i++ {
		l, rest := ExtractLength(membersArray)
		members[i] = rest[:l]
		membersArray = rest[l:]
	}
	return members
}

func MembersToMap(members [][]byte) map[string]string {
	m := make(map[string]string)
	for i := 0; i < len(members); i += 2 {
		m[string(members[i])] = string(members[i+1])
	}
	return m
}

func MapToMembers(m map[string]string) [][]byte {
	members := make([][]byte, 0)
	for k, v := range m {
		members = append(members, []byte(k))
		members = append(members, []byte(v))
	}
	return members
}

func MembersToSet(members [][]byte) (map[string]struct{}, int) {
	return AddMembersToSet(make(map[string]struct{}), members)
}

func AddMembersToSet(set map[string]struct{}, newMembers [][]byte) (map[string]struct{}, int) {
	originalLength := len(set)
	for i := 0; i < len(newMembers); i++ {
		set[string(newMembers[i])] = struct{}{}
	}
	return set, len(set) - originalLength
}

func SetToMembers(s map[string]struct{}) [][]byte {
	members := make([][]byte, 0)
	for k, _ := range s {
		members = append(members, []byte(k))
	}
	return members
}
