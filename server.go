package raftis

import (
	"fmt"
	"github.com/jbooth/flotilla"
	mdb "github.com/jbooth/gomdb"
	ops "github.com/jbooth/raftis/ops"
	redis "github.com/jbooth/raftis/redis"
	log "github.com/jbooth/raftis/rlog"
	"io"
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
		//SETEX
		"APPEND": ops.APPEND,
		"INCR":   ops.INCR,
		"DECR":   ops.DECR,
		"INCRBY": ops.INCRBY,
		"DECRBY": ops.DECRBY,
		"DEL":    ops.DEL,
		// lists
		"RPUSH": ops.RPUSH,
		// LPUSH
		// LTRIM
		// LSET
		// LREM
		// LPOP
		// RPOP
		// LPUSHX
		// RPUSHX
		// BLPOP
		// BRPOP
		// hashes
		"HSET":    ops.HSET,
		"HMSET":   ops.HMSET,
		"HINCRBY": ops.HINCRBY,
		"HDEL":    ops.HDEL,
		// sets
		// SADD
		// ttl
		"EXPIRE": ops.EXPIRE,
		//EXPIREAT
		// noop is for sync requests
		"PING": func(args [][]byte, txn *mdb.Txn) ([]byte, error) { return []byte("+PONG\r\n"), nil },
	}

	readOps = map[string]readOp{
		"GET":    ops.GET,
		"STRLEN": ops.STRLEN,
		"EXISTS": ops.EXISTS,
		//TYPE
		// lists
		"LLEN": ops.LLEN,
		// LRANGE
		// LINDEX

		// hashes
		"HGET":    ops.HGET,
		"HMGET":   ops.HMGET,
		"HGETALL": ops.HGETALL,
		// HEXISTS
		// HLEN
		// HKEYS
		// HVALS
		// sets
		// SMEMBERS
		// SCARD
		// SISMEMBER
		// SRANDMEMBER
		// ttl
		"TTL": ops.TTL,
	}
)

type Server struct {
	cluster  *ClusterMember
	flotilla flotilla.DB
	redis    *net.TCPListener
	lg       *log.Logger
}

func NewServer(c *ClusterConfig, dataDir string) (*Server, error) {
	lg := log.New(os.Stderr, fmt.Sprintf("Raftis %s:\t", c.Me.RedisAddr), log.LstdFlags)
	// find our replicaset
	var ours []Host = nil
	for _, s := range c.Shards {
		for _, h := range s.Hosts {
			if h.RedisAddr == c.Me.RedisAddr && h.FlotillaAddr == c.Me.FlotillaAddr {
				ours = s.Hosts
			}
		}
	}
	if ours == nil {
		return nil, fmt.Errorf("Host %+v not in hosts %+v", c.Me, c.Shards)
	}
	flotillaPeers := make([]string, len(ours), len(ours))
	for idx, h := range ours {
		flotillaPeers[idx] = h.FlotillaAddr
	}

	// start flotilla
	// peers []string, dataDir string, bindAddr string, ops map[string]Command
	f, err := flotilla.NewDefaultDB(flotillaPeers, dataDir, c.Me.FlotillaAddr, writeOps)
	if err != nil {
		return nil, err
	}
	// connect to cluster
	cl, err := NewClusterMember(c, lg)
	if err != nil {
		return nil, fmt.Errorf("Err connecting to cluster %s", err)
	}

	// start listening on redis port
	redisAddr, err := net.ResolveTCPAddr("tcp4", c.Me.RedisAddr)
	if err != nil {
		return nil, fmt.Errorf("Couldn't resolve redisAddr %s : %s", c.Me.RedisAddr, err)
	}
	redisListen, err := net.ListenTCP("tcp4", redisAddr)
	if err != nil {
		return nil, fmt.Errorf("Couldn't bind  to redisAddr %s", c.Me.RedisAddr, err)
	}
	s := &Server{cl, f, redisListen, lg}
	return s, nil
}

func (s *Server) Serve() (err error) {
	defer func(s *Server) {
		s.redis.Close()
		s.flotilla.Close()
		s.lg.Printf("server on %s going down: %s", s.redis.Addr().String(), err)
	}(s)
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
	if len(r.Args) > 0 {
		hasKey, err := s.cluster.HasKey(r.Args[0])
		s.lg.Printf("Local for key %s ? %s", r.Args[0], hasKey)
		if err != nil {
			return redis.NewError(fmt.Sprintf("error checking key status for key %s : %s", r.Args[0], err))
		}
		if !hasKey {
			// we don't have key locally, forward to correct node
			fwd, err := s.cluster.ForwardCommand(r.Name, r.Args)
			if err != nil {
				return redis.NewError(fmt.Sprintf("Error forwarding command: %s", err.Error()))
			}
			return fwd
		}
	}
	// have the key locally, apply command or execute read
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
	return redis.NewError(fmt.Sprintf("Unknown command %s", r.Name))
}

type pendingWrite struct {
	r <-chan flotilla.Result
}

func (p pendingWrite) WriteTo(w io.Writer) (int64, error) {
	resp := <-p.r
	// wrap any error as a response to client
	if resp.Err != nil {
		return redis.NewError(resp.Err.Error()).WriteTo(w)
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
		return redis.NewError(err.Error()).WriteTo(w)
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
		return redis.NewError(noopResp.Err.Error()).WriteTo(w)
	}
	// handle as normal read
	return p.r.WriteTo(w)
}

func (s *Server) Close() error {
	s.redis.Close()
	return s.flotilla.Close()
}
