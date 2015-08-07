package raftis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"github.com/jbooth/flotilla"
	mdb "github.com/jbooth/gomdb"
	config "github.com/jbooth/raftis/config"
	ops "github.com/jbooth/raftis/ops"
	redis "github.com/jbooth/raftis/redis"
	log "github.com/jbooth/raftis/rlog"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// writes a valid redis protocol response to the supplied Writer, returning bytes written, err
type readOp func(args [][]byte, txn *mdb.Txn, w io.Writer) (int64, error)
type serverOp func(args [][]byte, c *Conn, s *Server) io.WriterTo

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
		"SADD": ops.SADD,
		// ttl
		"EXPIRE": ops.EXPIRE,
		//EXPIREAT
		// pseudo lua scripting :)
		"EVAL": ops.EVAL,
		// noop is for sync requests
		"PING": func(args [][]byte, txn *mdb.Txn) ([]byte, error) {
			txn.Abort()
			return []byte("+PONG\r\n"), nil
		},
	}

	readOps = map[string]readOp{
		"GET":    ops.GET,
		"STRLEN": ops.STRLEN,
		"EXISTS": ops.EXISTS,
		//TYPE
		// lists
		"LLEN":   ops.LLEN,
		"LRANGE": ops.LRANGE,
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
		"SMEMBERS": ops.SMEMBERS,
		"SCARD":    ops.SCARD,
		// SISMEMBER
		// SRANDMEMBER
		// ttl
		"TTL": ops.TTL,
	}

	serverOps = map[string]serverOp{
		"CONFIG":     handleConfig,
		"SYNCMODE":   dosync,
		"NOSYNCMODE": donosync,
		"FATAL":      fatal,
		"STATS":      stats,
	}
)

type Server struct {
	cluster  *ClusterMember
	etcdC    *etcd.Client
	flotilla flotilla.DB
	redis    *net.TCPListener
	lg       *log.Logger
	stats    *StatsCounter
}

func NewServer(c *config.ClusterConfig,
	debugLogging bool) (*Server, error) {

	lg := log.New(
		os.Stderr,
		fmt.Sprintf("Raftis %s:\t", c.Me.RedisAddr),
		log.LstdFlags,
		debugLogging)
	etcdClient := etcd.NewClient([]string{c.Etcd})
	etcd.SetLogger(lg.WrappedLogger.Logger)

	// find our replicaset
	var ours []config.Host = nil
	for _, s := range c.Shards {
		for _, h := range s.Hosts {
			if h.RedisAddr == c.Me.RedisAddr && h.FlotillaAddr == c.Me.FlotillaAddr {
				ours = s.Hosts
			}
		}
	}
	if ours == nil {
		lg.Printf("Host %+v host in hosts %+v", c.Me, c.Shards)
		time.Sleep(1e6 * time.Second)
		return nil, fmt.Errorf("Host %+v not in hosts %+v", c.Me, c.Shards)
	}

	flotillaPeers := make([]string, len(ours), len(ours))
	for idx, h := range ours {
		flotillaPeers[idx] = h.FlotillaAddr
	}

	flotillaListen, err := net.Listen("tcp", c.Me.FlotillaAddr)
	if err != nil {
		return nil, err
	}
	// start flotilla
	dialer := &dialer{
		&net.Dialer{
			Timeout:   5 * time.Minute,
			LocalAddr: nil,
			DualStack: false,
			KeepAlive: 100 * time.Second * 86400,
		},
	}
	f, err := flotilla.NewDB(
		flotillaPeers,
		c.Datadir,
		flotillaListen, dialer.Dial, writeOps, lg.WrappedLogger.Logger)

	if err != nil {
		return nil, err
	}
	// connect to cluster
	cl, err := NewClusterMember(c, lg)
	if err != nil {
		return nil, fmt.Errorf("Err connecting to cluster %s", err)
	}
	// start heartbeat and cluster refresh

	// start listening on redis port
	redisAddr, err := net.ResolveTCPAddr("tcp4", c.Me.RedisAddr)
	if err != nil {
		return nil, fmt.Errorf("Couldn't resolve redisAddr %s : %s", c.Me.RedisAddr, err)
	}
	redisListen, err := net.ListenTCP("tcp4", redisAddr)
	if err != nil {
		return nil, fmt.Errorf("Couldn't bind  to redisAddr %s", c.Me.RedisAddr, err)
	}
	stats := &StatsCounter{
		currInterval:    NewStatsInterval(),
		l:               &sync.Mutex{},
		ticker:          time.NewTicker(5 * time.Second), // every 5 seconds
		diskTotal:       totalDiskSpace(),
		serverStartTime: time.Now().Unix(),
	}
	s := &Server{cl, etcdClient, f, redisListen, lg, stats}
	// update heartbeats and config
	go func() {
		for _ = range stats.ticker.C {
			// get stats
			collected := stats.collectInterval()
			// get config
			s.cluster.l.RLock()
			etcdBase := s.cluster.c.EtcdBase
			me := s.cluster.c.Me
			prevConfig := s.cluster.c
			s.cluster.l.RUnlock()
			confResp, err := s.etcdC.Get(etcdBase+"/shards", false, false)
			if err != nil {
				panic("Lost contact with etcd in heartbeat!")
			}
			var shards []config.Shard
			err = json.Unmarshal([]byte(confResp.Node.Value), &shards)
			if err != nil {
				panic(fmt.Errorf("Error unmarshalling shards from etcd!  resp: %s", confResp.Node.Value))
			}
			// update config if necessary
			if !config.ShardsEqual(shards, prevConfig.Shards) {
				s.cluster.l.Lock()
				s.cluster.c.Shards = shards
				myShard := s.cluster.c.MyShard()
				newPeers := make(map[string]bool)
				for _, h := range myShard.Hosts {
					newPeers[h.FlotillaAddr] = true
				}
				for _, h := range prevConfig.MyShard().Hosts {
					_, stillHere := newPeers[h.FlotillaAddr]
					if !stillHere {
						addr, err := net.ResolveTCPAddr("tcp", h.FlotillaAddr)
						if err != nil {
							panic(err)
						}
						err = s.flotilla.RemovePeer(addr)
						if err != nil {
							lg.Printf("Error removing peer %s : %s\n", h.FlotillaAddr, err)
						}
					}
				}

				s.cluster.l.Unlock()
			}

			// update heartbeat
			heartBeatKey := etcdBase + "/nodes/" + me.RedisAddr + "/heartbeat"
			heartBeatVal, err := json.Marshal(collected)
			if err != nil {
				panic(err)
			}
			s.etcdC.Set(heartBeatKey, string(heartBeatVal), 30) // TTL of 30, with updates every 5
			//lg.Printf("Collected stats interval %s on %s", collected.String(), t.String())
		}
	}()

	return s, nil
}

func (s *Server) joinAbandonedShard(etcdC *etcd.Client, origConfig *config.ClusterConfig) *config.ClusterConfig {
	return nil
}

func (s *Server) Serve() (err error) {
	defer func(s *Server) {
		s.redis.Close()
		s.flotilla.Close()
		s.lg.Printf("server on %s going down: %s", s.redis.Addr().String(), err)
		return
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

type dialer struct {
	d *net.Dialer
}

func (d *dialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return d.d.Dial("tcp", address)
}

var get []byte = []byte("GET")

func (s *Server) doRequest(c *Conn, r *redis.Request) io.WriterTo {
	serverOp, ok := serverOps[r.Name]
	if ok {
		return serverOp(r.Args, c, s)
	}
	hasKey, err := s.cluster.HasKey(r.Name, r.Args)
	if err != nil {
		keyStr := "NONE"
		if r.Args != nil && len(r.Args) > 0 {
			keyStr = string(r.Args[0])
		}
		s.lg.Errorf("error checking key status for key %s : %s", keyStr, err)
		return redis.NewError(fmt.Sprintf("error checking key status for key %s : %s", keyStr, err))
	}
	if !hasKey {
		// we don't have key locally, forward to correct node
		s.stats.incrNumForwards()
		fwd, err := s.cluster.ForwardCommand(r.Name, r.Args)
		if err != nil {
			return redis.NewError(fmt.Sprintf("Error forwarding command: %s", err.Error()))
		}
		return fwd
	}
	// have the key locally, apply command or execute read
	_, ok = writeOps[r.Name]
	if ok {
		s.stats.incrNumWrites()
		return pendingWrite{s.flotilla.Command(r.Name, r.Args)}
	}
	readOp, ok := readOps[r.Name]
	if ok {
		s.stats.incrNumReads()
		r := pendingRead{readOp, r.Args, s}
		if c.syncRead {
			return pendingSyncRead{s.flotilla.Command("PING", emptyArgs), r}
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

type txnReader interface {
	WriteTxnTo(t *mdb.Txn, w io.Writer) (int64, error)
}

type pendingRead struct {
	op   readOp
	args [][]byte
	s    *Server
}

func (p pendingRead) WriteTxnTo(t *mdb.Txn, w io.Writer) (int64, error) {
	return p.op(p.args, t, w)
}
func (p pendingRead) WriteTo(w io.Writer) (int64, error) {
	txn, err := p.s.flotilla.Read()
	if err != nil {
		return redis.NewError(err.Error()).WriteTo(w)
	}
	defer txn.Abort()
	return p.WriteTxnTo(txn, w)
}

type pendingSyncRead struct {
	noop <-chan flotilla.Result
	r    pendingRead
}

func (p pendingSyncRead) WriteTxnTo(t *mdb.Txn, w io.Writer) (int64, error) {
	// wait for no-op to sync
	noopResp := <-p.noop
	if noopResp.Err != nil {
		return redis.NewError(noopResp.Err.Error()).WriteTo(w)
	}
	// handle as normal read
	return p.r.WriteTxnTo(t, w)
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
	s.stats.ticker.Stop()
	s.redis.Close()
	return s.flotilla.Close()
}

func handleConfig(args [][]byte, c *Conn, s *Server) io.WriterTo {
	if len(args) > 0 && strings.ToUpper(string(args[0])) == "GET" {
		var resp redis.ReplyWriter
		if len(args) == 1 {
			resp = redis.NewError("ERR Wrong number of arguments for CONFIG GET")
		} else {
			ret := make([][]byte, 0)
			if bytes.Equal([]byte(strings.ToLower(string(args[1]))), []byte("cluster")) {
				var buf bytes.Buffer
				config.WriteConfig(s.cluster.c, &buf)
				ret = append(ret, []byte("cluster"))
				ret = append(ret, buf.Bytes())
			}
			resp = &redis.ArrayReply{ret}
		}
		return resp
	} else {
		return redis.NewError(fmt.Sprintf("Unrecognized CONFIG command %+v", args))
	}
}

func dosync(args [][]byte, c *Conn, s *Server) io.WriterTo {
	c.syncRead = true
	return &redis.StatusReply{"OK"}
}

func donosync(args [][]byte, c *Conn, s *Server) io.WriterTo {
	c.syncRead = false
	return &redis.StatusReply{"OK"}
}

func fatal(args [][]byte, c *Conn, s *Server) io.WriterTo {
	if len(args) == 0 {
		return redis.NewFatal("FATAL!  No msg")
	}
	return redis.NewFatal(string(args[0]))
}

func stats(args [][]byte, c *Conn, s *Server) io.WriterTo {
	ret := make([][]byte, 0)
	ret = append(ret, []byte(fmt.Sprintf("server start time: %d", s.stats.serverStartTime)))
	ret = append(ret, []byte(fmt.Sprintf("total disk space: %d bytes", s.stats.diskTotal)))
	ret = append(ret, []byte(fmt.Sprintf("current interval:\n %s", s.stats.currInterval.String())))
	return &redis.ArrayReply{ret}
}
