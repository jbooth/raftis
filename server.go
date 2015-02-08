package raftis

import (
	"fmt"
	redis "github.com/docker/go-redis-server"
	"github.com/jbooth/flotilla"
	mdb "github.com/jbooth/gomdb"
	ops "github.com/jbooth/raftis/ops"
	"io"
	"log"
	"net"
	"os"
)

// writes a valid redis protocol response to the supplied Writer, returning bytes written, err
type readOp func(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error)

var emptyBytes = make([]byte, 0)
var emptyArgs = make([][]byte, 0)

var (
	writeOps = map[string]flotilla.Command{
		"SET":    ops.SET,
		"GETSET": ops.GETSET,
		"SETNX":  ops.SETNX,
		// noop is for sync requests
		"NOOP": func(args [][]byte, txn *mdb.Txn) ([]byte, error) { return emptyBytes, nil },
	}

	readOps = map[string]readOp{
		"GET": ops.GET,
	}
)

type Server struct {
	flotilla flotilla.DB
	redis    *net.TCPListener
	lg       *log.Logger
}

func NewServer(redisBind string, flotillaBind string, dataDir string, flotillaPeers []string) (*Server, error) {
	lg := log.New(os.Stderr, fmt.Sprintf("Raftis %s:\t", redisBind), log.LstdFlags)
	// start flotilla
	// peers []string, dataDir string, bindAddr string, ops map[string]Command
	f, err := flotilla.NewDefaultDB(flotillaPeers, dataDir, flotillaBind, writeOps)
	if err != nil {
		return nil, err
	}
	// listen on redis port
	redisAddr, err := net.ResolveTCPAddr("tcp4", redisBind)
	if err != nil {
		return nil, fmt.Errorf("Couldn't resolve redisBind %s : %s", redisBind, err)
	}
	redisListen, err := net.ListenTCP("tcp4", redisAddr)
	if err != nil {
		return nil, fmt.Errorf("Couldn't bind  to redisAddr %s", redisBind, err)
	}
	s := &Server{f, redisListen, lg}
	// dispatch server
	go func() {
		err = s.serve()
		if err != nil {
			lg.Printf("server on %s going down: %s", redisBind, err)
		}
	}()
	return s, nil
}

func (s *Server) serve() (err error) {
	defer s.redis.Close()
	defer s.flotilla.Close()
	for {
		c, err := s.redis.AcceptTCP()
		if err != nil {
			return err
		}
		c.SetNoDelay(true)
		conn := NewConn(c)
		go conn.serveClient(s)
	}
}

func (s *Server) doRequest(c Conn, r *redis.Request) io.WriterTo {
	_, ok := writeOps[r.Name]
	if ok {
		return pendingWrite{s.flotilla.Command(r.Name, r.Args)}
	}
	readOp, ok := readOps[r.Name]
	if ok {
		r := pendingRead{readOp, r.Args, s}
		if c.syncRead {
			return pendingSyncRead{s.flotilla.Command("NOOP", emptyArgs), r}
		} else {
			return r
		}
	}
	// TODO use proper lib for this
	return ops.ErrorReply{fmt.Errorf("Unknown command %s", r.Name)}
}

type pendingWrite struct {
	r <-chan flotilla.Result
}

func (p pendingWrite) WriteTo(w io.Writer) (int64, error) {
	resp := <-p.r
	// wrap any error as a response to client
	if resp.Err != nil {
		return ops.ErrorReply{resp.Err}.WriteTo(w)
	}
	n, err := w.Write(resp.Response)
	return int64(n), err
}

type pendingRead struct {
	op   readOp
	args [][]byte
	s    *Server
}

func (p pendingRead) WriteTo(w io.Writer) (int64, error) {
	txn, err := p.s.flotilla.Read()
	if err != nil {
		return ops.ErrorReply{err}.WriteTo(w)
	}
	defer txn.Abort()
	return p.op(p.args, txn, w)
}

type pendingSyncRead struct {
	noop <-chan flotilla.Result
	r    pendingRead
}

func (p pendingSyncRead) WriteTo(w io.Writer) (int64, error) {
	// wait for no-op to sync
	noopResp := <-p.noop
	if noopResp.Err != nil {
		return ops.ErrorReply{noopResp.Err}.WriteTo(w)
	}
	// handle as normal read
	return p.r.WriteTo(w)
}

func (s *Server) Close() error {
	s.redis.Close()
	return s.flotilla.Close()
}
