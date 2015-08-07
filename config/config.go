package config

import (
	"encoding/json"
	"io"
	"os"
)

type ClusterConfig struct {
	NumSlots uint32  `json:"numSlots"`
	Me       Host    `json:"me"`          // should match a host in one of our shards exactly to identify which peergroup we join
	Etcd     string  `json: "etcd"`       // etcd host, must not have trailing slash
	EtcdBase string  `json: "etcdShards"` // etcd base node, like /raftis/myClusterName, no trailing slash
	Datadir  string  `json:"dataDir"`     // local data directory
	Shards   []Shard `json:"shards"`      // defines topography of cluster
}

func (c *ClusterConfig) MyShard() Shard {
	for _, s := range c.Shards {
		for _, h := range s.Hosts {
			if h.RedisAddr == c.Me.RedisAddr {
				return s
			}
		}
	}
	return Shard{}
}

func ShardsEqual(ss1 []Shard, ss2 []Shard) bool {
	if len(ss1) != len(ss2) {
		return false
	}
	for idx, s1 := range ss1 {
		s2 := ss2[idx]
		if s1.ShardId != s2.ShardId {
			return false
		}
		for j, slot := range s1.Slots {
			if s2.Slots[j] != slot {
				return false
			}
		}
		for j, h := range s1.Hosts {
			if s2.Hosts[j] != h {
				return false
			}
		}
	}
	return true
}

type Shard struct {
	ShardId int      `json: "id"`   // invalid if negative
	Slots   []uint32 `json:"slots"` // which slots this shard owns
	Hosts   []Host   `json:"hosts"` // which hosts are currently serving this shard
}

type Host struct {
	RedisAddr    string `json:"redisAddr"`    // "192.168.0.4:8369"
	FlotillaAddr string `json:"flotillaAddr"` // "192.168.0.4:1103"
	Group        string `json:"group"`
}

func WriteConfig(c *ClusterConfig, w io.Writer) error {
	e := json.NewEncoder(w)
	return e.Encode(c)
}

func WriteConfigFile(c *ClusterConfig, filePath string) error {
	w, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0755))
	if err != nil {
		return err
	}
	defer w.Close()
	return WriteConfig(c, w)
}

func ReadConfig(r io.Reader) (*ClusterConfig, error) {
	d := json.NewDecoder(r)
	ret1 := &ClusterConfig{}
	retErr := d.Decode(ret1)
	return ret1, retErr
}

func ReadConfigFile(filePath string) (*ClusterConfig, error) {
	r, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ReadConfig(r)
}
