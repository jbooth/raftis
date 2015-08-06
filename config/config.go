package config

import (
	"encoding/json"
	"io"
	"os"
)

type ClusterConfig struct {
	NumSlots uint32  `json:"numSlots"`
	Me       Host    `json:"me"`      // should match a host in one of our shards exactly to identify which peergroup we join
	Datadir  string  `json:"dataDir"` // local data directory
	Shards   []Shard `json:"shards"`  // defines topography of cluster
}

type Shard struct {
	ShardId int,   `json: "id"`  // invalid if negative
	Slots []uint32 `json:"slots"` // which slots this shard owns
	Hosts []Host   `json:"hosts"` // which hosts are currently serving this shard
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
