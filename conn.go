package raftis

import (
	"bufio"
	redis "github.com/jbooth/raftis/redis"
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
	go sendResponses(responses, conn, s)

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

func sendResponses(resps chan io.WriterTo, conn net.Conn, s *Server) {
	defer conn.Close()
	var n int64 = 0
	var dirty bool = false
	txn, err := s.flotilla.Read()
	defer txn.Abort()
	if err != nil {
		// TODO write err to client
		s.lg.Printf("ERROR getting initial transaction for reads: %s", err.Error())
		return
	}
	for r := range resps {
		tr, isRead := r.(txnReader)
		if isRead {
			// only renew our TXN handle if dirty, otherwise just reuse and save some calls
			if dirty {
				err = txn.Renew()
				if err != nil {
					s.lg.Printf("ERROR renewing txn on dirty read: %s", err.Error())
					// TODO write err to client
					return
				}
				dirty = false
			}
			// txn handle was either recycled or renewed, execute the read
			n, err = tr.WriteTxnTo(txn, conn)
		} else {
			// write or administrative command, mark existing read-handle dirty and handle thru WriterTo interface
			dirty = true
			txn.Reset()
			n, err = r.WriteTo(conn)
		}
		if err != nil {
			s.lg.Printf("Error writing to %s, closing.. wrote %d bytes, err: %s", conn.RemoteAddr().String(), n, err)
			// TODO write err to client?  already broken
			return
		}
	}
}
