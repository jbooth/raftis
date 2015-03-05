package ops

import (
	mdb "github.com/jbooth/gomdb"
	dbwrap "github.com/jbooth/raftis/dbwrap"
	redis "github.com/jbooth/raftis/redis"
	"io"
	"strconv"
)

func LLEN(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	//todo: check args ,if not enough return "ERR wrong number of arguments for 'rpush' command"

	key := args[0]
	println("LLEN", string(key))
	rawList, err := dbwrap.GetRawList(txn, key)
	var resp redis.ReplyWriter
	if err == mdb.NotFound {
		// Redis returns 0 for non-existing key
		resp = &redis.IntegerReply{0}
	} else if err != nil {
		// write error
		resp = redis.NewError(err.Error())
	} else {
		length, _ := dbwrap.ExtractLength(rawList)
		// Redis returns 1 for existing key
		resp = &redis.IntegerReply{int(length)}
	}
	// write result
	return resp.WriteTo(w)

}

func toIntArg(raw []byte) (int, error) {
	return strconv.Atoi(string(raw))
}

func LRANGE(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error) {
	//todo: check args ,if not enough or too much return "ERR wrong number of arguments for 'LRANGE' command"

	key := args[0]
	start, err := toIntArg(args[1])
	if err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	end, err := toIntArg(args[2])
	if err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	println("LRANGE", string(key), start, end)

	var resp redis.ReplyWriter
	if start > end || start < 0 {
		return redis.NilArrayReply.WriteTo(w)
	}

	rawList, err := dbwrap.GetRawList(txn, key)
	if err == mdb.NotFound {
		// Redis returns 0 for non-existing key
		return redis.NilArrayReply.WriteTo(w)
	} else if err != nil {
		// write error
		resp = redis.NewError(err.Error())
	} else {
		length, membersList := dbwrap.ExtractLength(rawList)
		members := dbwrap.MembersArrayToMembers(length, membersList)
		if end > int(length) {
			end = int(length)
		} else {
			end++
		}
		membersRange := members[start:end]
		resp = &redis.ArrayReply{membersRange}
	}
	// write result
	return resp.WriteTo(w)
}
