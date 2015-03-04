package raftis

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
)

func NewClusterMember(c *ClusterConfig, lg *log.Logger) (*ClusterMember, error) {
	slotHosts := make(map[int32][]Host)
	for _, shard := range c.Shards {
		for _, slot := range shard.Slots {
			slotHosts[int32(slot)] = shard.Hosts
		}
	}
	// just set up hostConns all at once for now
	var err error
	hostConns := make(map[string]*PassthruConn)
	for _, hosts := range slotHosts {
		for _, h := range hosts {
			hostConns[h.RedisAddr], err = NewPassThru(h.RedisAddr)
			if err != nil {
				// lol fail to start if we can't reach any single host -- every point of failure
				hostConns[h.RedisAddr] = nil
				lg.Printf("Failed to connect to host %s : %s", h.RedisAddr, err.Error())
			}
		}
	}
	return &ClusterMember{
		&sync.RWMutex{},
		c,
		slotHosts,
		hostConns,
	}, nil

}

type ClusterMember struct {
	l         *sync.RWMutex
	c         *ClusterConfig
	slotHosts map[int32][]Host
	hostConns map[string]*PassthruConn // for forwarding commands when we don't have a key
}

func hash(key []byte) int32 {
	if key == nil {
		return 0
	}
	sum := int32(0)
	for i := 0; i < len(key); i++ {
		sum = (sum * 17) + int32(key[i])
	}
	return sum
}

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
		&sync.Mutex{},
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
		fmt.Printf("got response line: %s\n", string(line))
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
