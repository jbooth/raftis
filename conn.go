package raftis

import (
	"bufio"
	redis "github.com/jbooth/raftis/redis"
	rlog "github.com/jbooth/raftis/rlog"
	"io"
	"net"
	"strings"
)

type Conn struct {
	net.Conn
	syncRead bool
}

func NewConn(c net.Conn) *Conn {
	return &Conn{c, false}
}

type waiter interface {
	waitDone()
}

func (conn *Conn) serveClient(s *Server) (err error) {
	responses := make(chan io.WriterTo, 32)
	defer func() {
		close(responses)
	}()
	// dispatch response writer
	go sendResponses(responses, conn, s.lg)

	connRead := bufio.NewReader(conn)
	// read requests
	for {
		request, err := redis.ParseRequest(connRead, s.lg)
		if err != nil {
			return err
		}
		request.Host = conn.RemoteAddr().String()
		request.Name = strings.ToUpper(request.Name)
		if request.Name == "QUIT" {
			break
		}

		// dispatch request
		response := s.doRequest(conn, request)
		// pass pending response to response writer
		responses <- response
		waiter, ok := response.(waiter)
		if ok {
			waiter.waitDone()
		}
	}
	return nil
}

func sendResponses(resps chan io.WriterTo, conn net.Conn, lg *rlog.Logger) {
	defer conn.Close()
	for r := range resps {
		n, err := r.WriteTo(conn)
		if err != nil {
			lg.Printf("Error writing to %s, closing.. wrote %d bytes, err: %s", conn.RemoteAddr().String(), n, err)
			return
		}
	}
}
