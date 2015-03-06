package raftis

import (
	"testing"
  config "github.com/jbooth/raftis/config"
)

func TestWriteReadConfig(t *testing.T) {

	cfg := &config.ClusterConfig{
		NumSlots: uint32(10),
		Me:   config.Host{"phx1.corp.ebay.com", "phx1.corp.ebay.com", "phx"},

		Shards: []config.Shard{
			config.Shard{
				Slots: []uint32{0, 3, 6, 9},
				Hosts: []config.Host{
					config.Host{"phx1.corp.ebay.com:8679", "phx1.corp.ebay.com:1103", "phx"},
					config.Host{"slc1.corp.ebay.com:8679", "slc1.corp.ebay.com:1103", "slc"},
					config.Host{"lvs1.corp.ebay.com:8679", "lvs1.corp.ebay.com:1103", "lvs"},
				},
			},
			config.Shard{
				Slots: []uint32{1, 4, 7},
				Hosts: []config.Host{
					config.Host{"phx2.corp.ebay.com:8679", "phx2.corp.ebay.com:1103", "phx"},
					config.Host{"slc2.corp.ebay.com:8679", "slc2.corp.ebay.com:1103", "slc"},
					config.Host{"lvs2.corp.ebay.com:8679", "lvs2.corp.ebay.com:1103", "lvs"},
				},
			},
			config.Shard{
				Slots: []uint32{2, 5, 8},
				Hosts: []config.Host{
					config.Host{"phx3.corp.ebay.com:8679", "phx3.corp.ebay.com:1103", "phx"},
					config.Host{"slc3.corp.ebay.com:8679", "slc3.corp.ebay.com:1103", "slc"},
					config.Host{"lvs3.corp.ebay.com:8679", "lvs3.corp.ebay.com:1103", "lvs"},
				},
			},
		},
	}
	err := config.WriteConfigFile(cfg, "/tmp/config.cfg")
	if err != nil {
		panic(err)
	}
}
