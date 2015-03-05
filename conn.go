package raftis

import (
	redis "github.com/jbooth/raftis/redis"
	"io"
  rlog "github.com/jbooth/raftis/rlog"
	"net"
	"strings"
)

type Conn struct {
	net.Conn
	syncRead bool
}

func NewConn(c net.Conn) Conn {
	return Conn{c, false}
}

func (conn Conn) serveClient(s *Server) (err error) {
	responses := make(chan io.WriterTo, 32)
	defer func() {
		close(responses)
	}()
	// dispatch response writer
	go sendResponses(responses, conn, s.lg)
	// read requests
	for {
		request, err := redis.ParseRequest(conn)
		if err != nil {
			return err
		}
		request.Host = conn.RemoteAddr().String()
		request.Name = strings.ToUpper(request.Name)
		s.lg.Printf("Got command %s", request.Name)

		// dispatch request
		response := s.doRequest(conn, request)
		// pass pending response to response writer
		responses <- response
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
