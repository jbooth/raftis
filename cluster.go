package raftis

import (
	"fmt"
	log "github.com/jbooth/raftis/rlog"
	"io"
	"sync"
  "github.com/jbooth/raftis/config"
)

func NewClusterMember(c *config.ClusterConfig, lg *log.Logger) (*ClusterMember, error) {
	slotHosts := make(map[int32][]config.Host)
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
	c         *config.ClusterConfig
	slotHosts map[int32][]config.Host
	hostConns map[string]*PassthruConn // for forwarding commands when we don't have a key
}

func (c *ClusterMember) HasKey(key []byte) (bool, error) {
	s := c.slotForKey(key)
	hosts, ok := c.slotHosts[s]
	if !ok {
		return false, fmt.Errorf("No hosts for slot %d", s)
	}
	for _, h := range hosts {
		if h.RedisAddr == c.c.Me.RedisAddr {
			return true, nil
		}
	}
	return false, nil
}

func (c *ClusterMember) ForwardCommand(cmdName string, args [][]byte) (io.WriterTo, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("Can't forward command %s, need at least 1 arg for key!", cmdName)
	}
	slot := c.slotForKey(args[0]) // this is wrong sometimes
	hosts, ok := c.slotHosts[slot]
	if !ok || len(hosts) == 0 {
		return nil, fmt.Errorf("No host for slot %d, key %s!", slot, string(args[0]))
	}
	conn := c.hostConns[hosts[0].RedisAddr]
	pending, err := conn.Command(cmdName, args)
	if err != nil {
		return nil, fmt.Errorf("Error forwarding command %s to host %s : %s", cmdName, hosts[0].RedisAddr, err.Error())
	}
	return &Forward{pending}, nil
}

type Forward struct {
	pending <-chan PassthruResp
}

func (f *Forward) WriteTo(w io.Writer) (int64, error) {
	resp := <-f.pending
	if resp.Err != nil {
		return 0, resp.Err
	}
	ret1, ret2 := w.Write(resp.Data)
	return int64(ret1), ret2
}

func (c *ClusterMember) slotForKey(key []byte) int32 {
	h := hash(key)
	if h < 0 {
		h = -h
	}
	return h % int32(c.c.NumSlots)
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
