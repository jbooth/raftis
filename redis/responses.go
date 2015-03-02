package redis

import (
	"io"
	"strconv"
)

func WrapStatus(code string) []byte {
	return []byte("+" + code + "\r\n")
}

func WrapInt(v int) []byte {
	return []byte(":" + strconv.Itoa(v) + "\r\n")
}

func WrapString(v []byte) []byte {
	if v == nil || len(v) == 0 {
		return []byte("$-1\r\n")
	}
	header := []byte("$" + strconv.Itoa(len(v)) + "\r\n")
	ret := make([]byte, len(header)+len(v)+2)
	copy(ret, header)
	copy(ret[len(header):], v)
	ret[len(ret)-2] = byte('\r')
	ret[len(ret)-1] = byte('\n')
	return ret
}

func WrapNil() []byte {
	return WrapString(nil)
}

func ReplyString(w io.Writer, v []byte) (int64, error) {
	if v == nil || len(v) == 0 {
		n, err := w.Write([]byte("$-1\r\n"))
		return int64(n), err
	}
	wrote, err := w.Write([]byte("$" + strconv.Itoa(len(v)) + "\r\n"))
	if err != nil {
		return int64(wrote), err
	}
	wroteBytes, err := w.Write(v)
	if err != nil {
		return int64(wrote + wroteBytes), err
	}
	wroteCrLf, err := w.Write([]byte("\r\n"))
	return int64(wrote + wroteBytes + wroteCrLf), err
}

type StringReply struct {
	V []byte
}

func (r StringReply) WriteTo(w io.Writer) (int64, error) {

	if r.V == nil || len(r.V) == 0 {
		n, err := w.Write([]byte("$-1\r\n"))
		return int64(n), err
	}
	wrote, err := w.Write([]byte("$" + strconv.Itoa(len(r.V)) + "\r\n"))
	if err != nil {
		return int64(wrote), err
	}
	wroteBytes, err := w.Write(r.V)
	if err != nil {
		return int64(wrote + wroteBytes), err
	}
	wroteCrLf, err := w.Write([]byte("\r\n"))
	return int64(wrote + wroteBytes + wroteCrLf), err
}
