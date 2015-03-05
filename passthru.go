package raftis

import (
	"bufio"
	"net"
	"strconv"
	"sync"
)

func NewPassThru(remoteHost string) (*PassthruConn, error) {
	conn, err := net.Dial("tcp", remoteHost)
	if err != nil {
		return nil, err
	}
	ret := &PassthruConn{
		make(chan chan PassthruResp),
		conn,
		bufio.NewReader(conn),
		bufio.NewWriter(conn),
		new(sync.Mutex),
	}
	go ret.routeResponses()
	return ret, nil
}

// threadsafe single conn multiplexer
// doCommand writes the request over the wire and returns a chan which
// will yield a single response of []byte,error (naively scanned until first '\n')
// does not support multiline responses yet
type PassthruConn struct {
	pendingResp    chan chan PassthruResp
	underlyingConn net.Conn
	bufIn          *bufio.Reader
	bufOut         *bufio.Writer
	l              *sync.Mutex
}

type PassthruResp struct {
	Data []byte
	Err  error
}

var crlf = []byte{byte('\r'), byte('\n')}

func (p *PassthruConn) Command(cmd string, args [][]byte) (<-chan PassthruResp, error) {
	p.l.Lock()
	defer p.l.Unlock()

	err := writeCmd(cmd, args, p.bufOut)
	if err != nil {
		return nil, err
	}

	// flush command, register receive chan
	receiver := make(chan PassthruResp, 1)
	p.pendingResp <- receiver
	return receiver, nil
}

func (p *PassthruConn) routeResponses() {
	var err error
	for resp := range p.pendingResp {
		// read from sock
		line, err := p.bufIn.ReadBytes('\n')
		if err != nil {
			resp <- PassthruResp{nil, err}
			break //
		}
		//fmt.Printf("got response line: %s\n", string(line))
		// forward on along
		resp <- PassthruResp{line, nil}
	}
	if err != nil {
		p.underlyingConn.Close()
		//repeat error to all pending responses
		for resp := range p.pendingResp {
			resp <- PassthruResp{nil, err}
		}
	}
}

// writes and flushes command to specified bufio.Writer
func writeCmd(cmd string, args [][]byte, out *bufio.Writer) error {
	// we send an array of bulkstrings for the command
	n := len(args) + 1
	err := out.WriteByte(byte('*'))
	if err != nil {
		return err
	}
	_, err = out.Write([]byte(strconv.FormatInt(int64(n), 10)))
	if err != nil {
		return err
	}
	_, err = out.Write(crlf)
	if err != nil {
		return err
	}

	// write name
	err = out.WriteByte(byte('$'))
	if err != nil {
		return err
	}
	_, err = out.Write([]byte(strconv.FormatInt(int64(len(cmd)), 10)))
	if err != nil {
		return err
	}
	_, err = out.Write(crlf)
	if err != nil {
		return err
	}
	_, err = out.WriteString(cmd)
	if err != nil {
		return err
	}
	_, err = out.Write(crlf)
	if err != nil {
		return err
	}

	// write each arg
	for _, arg := range args {
		err = out.WriteByte(byte('$'))
		if err != nil {
			return err
		}
		_, err = out.Write([]byte(strconv.FormatInt(int64(len(arg)), 10)))
		if err != nil {
			return err
		}
		_, err = out.Write(crlf)
		if err != nil {
			return err
		}
		_, err = out.Write(arg)
		if err != nil {
			return err
		}
		_, err = out.Write(crlf)
		if err != nil {
			return err
		}
	}
	// final crlf
	_, err = out.Write(crlf)
	if err != nil {
		return err
	}
	return out.Flush()

}
