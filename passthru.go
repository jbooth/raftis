package raftis

import (
	"bufio"
	"fmt"
	log "github.com/jbooth/raftis/rlog"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

func NewPassThru(remoteHost string, lg *log.Logger) (*PassthruConn, error) {
	conn, err := net.Dial("tcp", remoteHost)
	if err != nil {
		return nil, err
	}
	// go for sync mode
	_, err = conn.Write([]byte("*1\r\n$8\r\nSYNCMODE\r\n"))
	if err != nil {
		return nil, fmt.Errorf("Error writing SYNCMODE establishing conn to %s", remoteHost)
	}
	in := bufio.NewReader(conn)
	line, err := in.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("Error writing reading SYNCMODE resp while establishing conn to %s", remoteHost)
	}
	if line != "+OK\r\n" {
		return nil, fmt.Errorf("Bad response when switching to SYNCMODE on conn to %s : %s", remoteHost, line)
	}
	ret := &PassthruConn{
		make(chan *PassthruResp),
		conn,
		in,
		bufio.NewWriter(conn),
		new(sync.Mutex),
		lg,
		false,
	}
	go ret.routeResponses()
	return ret, nil
}

// threadsafe single conn multiplexer
// doCommand writes the request over the wire and returns a chan which
// will yield a single response of []byte,error (naively scanned until first '\n')
// does not support multiline responses yet
type PassthruConn struct {
	pendingResp chan *PassthruResp
	conn        net.Conn
	bufIn       *bufio.Reader
	bufOut      *bufio.Writer
	l           *sync.Mutex
	lg          *log.Logger
	closed      bool
}

var crlf = []byte{byte('\r'), byte('\n')}

func (p *PassthruConn) Command(cmd string, args [][]byte) (*PassthruResp, error) {
	p.l.Lock()
	defer p.l.Unlock()
	if p.closed {
		return nil, fmt.Errorf("Connection closed!")
	}

	err := p.conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		p.closeInternal()
		return nil, fmt.Errorf("Error setting write deadline in Command %s to %s", cmd, p.conn.RemoteAddr().String())
	}
	err = writeCmd(cmd, args, p.bufOut)
	if err != nil {
		p.closeInternal()
		return nil, err
	}
	err = p.conn.SetWriteDeadline(time.Time{}) // unset deadline
	if err != nil {
		p.closeInternal()
		return nil, fmt.Errorf("Error unsetting write deadline after successful Command %s to %s", cmd, p.conn.RemoteAddr().String())
	}

	// flush command, register receive chan

	ready := make(chan error)
	done := make(chan error)
	resp := &PassthruResp{ready, done, p, p.lg, cmd, args}
	err = sendCatch(p.pendingResp, resp)
	return resp, err
}

func sendCatch(s chan *PassthruResp, p *PassthruResp) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("%v", x)
		}
	}()
	err = nil
	s <- p
	return
}

func (p *PassthruConn) routeResponses() {
	defer p.Close()
	var err error = nil
	// we iterate in a loop, signaling ready on one chan then blocking till done on the other
	// actual pipelining of responses is done by the goroutine invoking WriteTo() on PassthruResp to send to the actual client,
	for resp := range p.pendingResp {
		p.lg.Printf("Processing command")
		// set read timeout for response
		err = p.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		if err != nil {
			p.lg.Printf("Error setting read deadline in readResponses from p.conn %s", p.conn.RemoteAddr().String())
			return
		}
		//signal ready
		p.lg.Printf("Signalling ready to respond to command")
		resp.ready <- err
		// wait done
		err = <-resp.done
		if err != nil {
			p.lg.Printf("Error processing cmd in conn to %s", p.conn.RemoteAddr().String())
			return
		}
		// unset read deadline
		err = p.conn.SetReadDeadline(time.Time{})
		if err != nil {
			p.lg.Printf("Error setting read deadline in readResponses from p.conn %s", p.conn.RemoteAddr().String())
			return
		}
	}
}

func (p *PassthruConn) Close() {
	p.l.Lock()
	defer p.l.Unlock()
	p.closeInternal()
}

func (p *PassthruConn) closeInternal() {
	if !p.closed {
		close(p.pendingResp)
		p.conn.Close()
		p.closed = true
	}
}

// writes and flushes command to specified bufio.Writer
func writeCmd(cmd string, args [][]byte, out *bufio.Writer) error {

	// now process command
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

type inAndErr struct {
	in  *bufio.Reader
	err error
}
type PassthruResp struct {
	ready    chan error
	done     chan error
	p        *PassthruConn
	lg       *log.Logger
	origCmd  string   // for debug
	origArgs [][]byte // for debug
}

// Forwards a remote command to a client.
func (p *PassthruResp) WriteTo(w io.Writer) (int64, error) {
	// wait till our turn
	err := <-p.ready
	if err != nil {
		p.lg.Printf("passthru ERR! %s\n", err)
		p.done <- err
		return 0, err
	}

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
