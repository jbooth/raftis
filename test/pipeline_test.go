package raftis

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
)

func TestPipeline(t *testing.T) {
	setupTest()
	requests := packCommand("SET", "FOO", "BAR")
	requests = append(requests, packCommand("SET", "BAR", "FOO")...)
	requests = append(requests, packCommand("SET", "BAZ", "BAZ")...)
	requests = append(requests, packCommand("SET", "BAA", "BAA")...)
	requests = append(requests, packCommand("GET", "FOO")...)
	requests = append(requests, packCommand("GET", "BAR")...)
	requests = append(requests, packCommand("SET", "BAB", "BAA")...)
	requests = append(requests, packCommand("GET", "BAB")...)
	requests = append(requests, packCommand("GET", "BAA")...)

	targetAddr := testcluster.hosts[0].RedisAddr
	conn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		panic(err)
	}

	conn.Write(requests)

	expectedResponse := "+OK\r\n+OK\r\n+OK\r\n$3\r\nBAR\r\n$3\r\nFOO\r\n+OK\r\n$3\r\nFOO\r\n+OK\r\n+OK\r\n"
	respBuff := make([]byte, len(expectedResponse), len(expectedResponse))
	_, err = io.ReadFull(conn, respBuff)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respBuff))
}

func packCommand(args ...interface{}) []byte {
	n := len(args)
	res := make([]byte, 0, 16*n)
	res = append(res, byte('*'))
	res = strconv.AppendInt(res, int64(n), 10)
	res = append(res, byte('\r'), byte('\n'))
	for _, arg := range args {
		res = append(res, byte('$'))
		switch v := arg.(type) {
		case []byte:
			res = strconv.AppendInt(res, int64(len(v)), 10)
			res = append(res, byte('\r'), byte('\n'))
			res = append(res, v...)
		case string:
			res = strconv.AppendInt(res, int64(len(v)), 10)
			res = append(res, byte('\r'), byte('\n'))
			res = append(res, []byte(v)...)
		case int:
			res = strconv.AppendInt(res, numLen(int64(v)), 10)
			res = append(res, byte('\r'), byte('\n'))
			res = strconv.AppendInt(res, int64(v), 10)
		case int64:
			res = strconv.AppendInt(res, numLen(v), 10)
			res = append(res, byte('\r'), byte('\n'))
			res = strconv.AppendInt(res, int64(v), 10)
		case uint64:
			res = strconv.AppendInt(res, numLen(int64(v)), 10)
			res = append(res, byte('\r'), byte('\n'))
			res = strconv.AppendUint(res, uint64(v), 10)
		case float64:
			var buf []byte
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
			res = strconv.AppendInt(res, int64(len(buf)), 10)
			res = append(res, byte('\r'), byte('\n'))
			res = append(res, buf...)
		default:
			panic("invalid argument type when pack command")
		}
		res = append(res, byte('\r'), byte('\n'))
	}
	return res
}

func numLen(i int64) int64 {
	n, pos10 := int64(1), int64(10)
	if i < 0 {
		i = -i
		n++
	}
	for i >= pos10 {
		n++
		pos10 *= 10
	}
	return n
}
