package raftis

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

func (c *ClusterMember) slotForKey(key string) int32 {
	h := hash(key)
	if h < 0 {
		h = -h
	}
	return h % int32(c.c.NumSlots)
}

func (c *ClusterMember) hasKey(key string) (bool,error) {
  s := slotForKey(key)
  hosts,ok := slotHosts[s]
  if !ok {
    return false,fmt.Errorf("No hosts for slot %d",
  }
  for _,h := range hosts {
    if h.RedisAddr == c.c.Me.RedisAddr {
      return true,nil
    }
  }
  return false,nil
}

func (c *ClusterMember) ForwardCommand(cmdName string, args [][]byte) {

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
