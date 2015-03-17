package raftis

import (
	"bufio"
	"fmt"
	"io"
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
		make(chan *PassthruResp),
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
	pendingResp    chan *PassthruResp
	underlyingConn net.Conn
	bufIn          *bufio.Reader
	bufOut         *bufio.Writer
	l              *sync.Mutex
}

var crlf = []byte{byte('\r'), byte('\n')}

func (p *PassthruConn) Command(cmd string, args [][]byte) (*PassthruResp, error) {
	p.l.Lock()
	defer p.l.Unlock()

	err := writeCmd(cmd, args, p.bufOut)
	if err != nil {
		return nil, err
	}

	// flush command, register receive chan

	ready := make(chan error)
	done := make(chan error)
	resp := &PassthruResp{ready, done, p}
	p.pendingResp <- resp
	return resp, nil
}

func (p *PassthruConn) routeResponses() {
	defer p.underlyingConn.Close()
	var err error = nil
	// we iterate in a loop, signaling ready on one chan then blocking till done on the other
	// actual pipelining of responses is done by the goroutine invoking WriteTo() on PassthruResp to send to the actual client,
	for resp := range p.pendingResp {

		//signal ready
		resp.ready <- err
		// wait done
		err = <-resp.done
		if err != nil {
			p.underlyingConn.Close()
			close(p.pendingResp)
		}
	}
}

// writes and flushes command to specified bufio.Writer
func writeCmd(cmd string, args [][]byte, out *bufio.Writer) error {
	//fmt.Printf("cmd writing ping\n")
	// first send a ping for consistency
	//_, err := out.WriteString("PING")
	//if err != nil {
	//return err
	//}
	//_, err = out.Write(crlf)
	//if err != nil {
	//return err
	//}

	// now process command
	// we send an array of bulkstrings for the command
	fmt.Printf("cmd writing array header\n")
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

	fmt.Printf("cmd writing cmd name\n")
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
		fmt.Printf("cmd writing arg\n")
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
	fmt.Printf("cmd writing final crlf\n")
	_, err = out.Write(crlf)
	if err != nil {
		return err
	}
	return out.Flush()

}

type inAndErr struct {
	in  *bufio.Reader
	err error
}
type PassthruResp struct {
	ready chan error
	done  chan error
	p     *PassthruConn
}

// Forwards a remote command to a client.
func (p *PassthruResp) WriteTo(w io.Writer) (int64, error) {
	// wait till our turn
	err := <-p.ready
	if err != nil {
		fmt.Printf("passthru ERR! %s\n", err)
		p.done <- err
		return 0, err
	}
	// pop off ping response "+PING\r\n"
	//pong := make([]byte, 7)
	//_, err = io.ReadFull(p.p.bufIn, pong)
	//fmt.Printf("ping response: %s\n", string(pong))
	//if err != nil {
	//return 0, err
	//}
	fmt.Printf("Forwarding response for orig command\n")

	// forward it along
	written, err := forwardResponse(p.p.bufIn, w)
	// signal done
	p.done <- err
	return int64(written), err
}

func forwardResponse(in *bufio.Reader, out io.Writer) (int, error) {
	// read first byte
	respType, err := in.ReadByte()
	if err != nil {
		return 0, err
	}
	// forward first byte
	written, err := out.Write([]byte{respType})
	if err != nil {
		return written, err
	}
	// send rest of message
	if respType == '+' || respType == ':' || respType == '-' {
		// simple strings, ints and errors are just one line
		w, err := fwdLine(in, out)
		written += w
		return written, err
	} else if respType == '$' {
		// bulk string, length on one line, then content on another
		length, w, err := readFwdInt(in, out)
		written += w
		if err != nil {
			return written, err
		}

		w2, err := io.CopyN(out, in, int64(length)+2) // + 2 to include final trailing \r\n
		written += int(w2)
		return written, err
	} else if respType == '*' {
		// array, num members on one line then N more full responses
		numMembers, w, err := readFwdInt(in, out)
		written += w
		if err != nil {
			return written, err
		}
		for i := 0; i < numMembers; i++ {
			w, err = forwardResponse(in, out)
			written += w
			if err != nil {
				return written, err
			}
		}
		return written, nil
	}
	return written, err
}

func fwdLine(in *bufio.Reader, out io.Writer) (int, error) {
	toFwd, err := in.ReadString('\n')
	if err != nil {
		return 1, err
	}
	written, err := out.Write([]byte(toFwd))
	return written + 1, err
}

// reads an int from the rest of this line, while forwarding it all to client.  returns int, fwded, error
func readFwdInt(in *bufio.Reader, out io.Writer) (int, int, error) {
	lenString, err := in.ReadBytes('\n')
	if err != nil {
		return 0, 0, err
	}
	// forward with '\r\n'
	w, err := out.Write(lenString)
	if err != nil {
		return 0, w, err
	}
	// pop off '\r\n' to convert to int
	lenString = lenString[:len(lenString)-2]
	length, err := strconv.Atoi(string(lenString))
	return length, w, err
}
