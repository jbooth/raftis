package raftis

import (
	"fmt"
	"github.com/jbooth/raftis/config"
	log "github.com/jbooth/raftis/rlog"
	"io"
	"sync"
)

func NewClusterMember(c *config.ClusterConfig, lg *log.Logger) (*ClusterMember, error) {
	slotHosts := make(map[int32][]config.Host)
	for _, shard := range c.Shards {
		for _, slot := range shard.Slots {
			slotHosts[int32(slot)] = shard.Hosts
		}
	}
	// just set up hostConns all at once for now
	hostConns := make(map[string]*PassthruConn)
	return &ClusterMember{
		lg,
		&sync.RWMutex{},
		c,
		slotHosts,
		hostConns,
	}, nil

}

type ClusterMember struct {
	lg        *log.Logger
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
	conn, err := c.getConnForKey(args[0])
	if err != nil {
		return nil, err
	}
	return conn.Command(cmdName, args)
}

func (c *ClusterMember) getConnForKey(key []byte) (*PassthruConn, error) {
	c.l.RLock()
	defer c.l.RUnlock()
	slot := c.slotForKey(key)
	hosts, ok := c.slotHosts[slot]
	if !ok {
		return nil, fmt.Errorf("No hosts configured for slot %d from key %s", slot, key)
	}
	hostsByGroup := make(map[string]config.Host)
	for _, host := range hosts {
		if host.RedisAddr == c.c.Me.RedisAddr {
			return nil, fmt.Errorf("Can't passthru to localhost!  Use the right interface.")
		}
		hostsByGroup[host.RedisAddr] = host
	}
	// try to favor same group
	sameGroup, hasSameGroup := hostsByGroup[c.c.Me.Group]
	var err error
	if hasSameGroup {
		c.lg.Printf("Connecting to host from same group %+v", sameGroup)
		sameGroupConn, err := c.getConnForHost(sameGroup.RedisAddr)
		if err == nil {
			c.lg.Printf("returning from same group")
			return sameGroupConn, nil
		} else {
			c.lg.Errorf("Error connecting to host %s for slot %d : %s", sameGroup.RedisAddr, slot, err.Error())
		}
	}
	// otherwise, randomly iterate till we find one that works
	for _, h := range hostsByGroup {
		conn, err := c.getConnForHost(h.RedisAddr)
		if err == nil {
			return conn, nil
		} else {
			c.lg.Errorf("Error connecting to host %+v for slot %d : %s", h, slot, err)
		}
	}
	return nil, fmt.Errorf("Couldn't find any hosts up for slot %d, key %s, hosts are %+v, error from last connect attempt: %s", slot, key, hosts, err)

}

// assumes Rlock is held
func (c *ClusterMember) getConnForHost(host string) (*PassthruConn, error) {
	conn, ok := c.hostConns[host]
	if ok {
		return conn, nil
	}
	// switch to writelock
	c.l.RUnlock()
	c.l.Lock()
	defer func() {
		c.l.Unlock()
		c.l.RLock()
	}()
	// doublecheck after writelocking
	conn, ok = c.hostConns[host]
	if ok {
		return conn, nil
	}
	conn, err := NewPassThru(host)
	if err == nil {
		c.hostConns[host] = conn
		return conn, nil
	} else {
		return nil, err
	}
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
