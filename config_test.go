package raftis

import (
	"testing"
)

func TestWriteReadConfig(t *testing.T) {

	cfg := &ClusterConfig{
		NumSlots: uint32(10),
		Whoami:   "phx1.corp.ebay.com",
		Shards: []Shard{
			Shard{
				Slots: []uint32{0, 3, 6, 9},
				Hosts: []Host{
					Host{"phx1.corp.ebay.com:8679", "phx1.corp.ebay.com:1103", "phx"},
					Host{"slc1.corp.ebay.com:8679", "slc1.corp.ebay.com:1103", "slc"},
					Host{"lvs1.corp.ebay.com:8679", "lvs1.corp.ebay.com:1103", "lvs"},
				},
			},
			Shard{
				Slots: []uint32{1, 4, 7},
				Hosts: []Host{
					Host{"phx2.corp.ebay.com:8679", "phx2.corp.ebay.com:1103", "phx"},
					Host{"slc2.corp.ebay.com:8679", "slc2.corp.ebay.com:1103", "slc"},
					Host{"lvs2.corp.ebay.com:8679", "lvs2.corp.ebay.com:1103", "lvs"},
				},
			},
			Shard{
				Slots: []uint32{2, 5, 8},
				Hosts: []Host{
					Host{"phx3.corp.ebay.com:8679", "phx3.corp.ebay.com:1103", "phx"},
					Host{"slc3.corp.ebay.com:8679", "slc3.corp.ebay.com:1103", "slc"},
					Host{"lvs3.corp.ebay.com:8679", "lvs3.corp.ebay.com:1103", "lvs"},
				},
			},
		},
	}
	err := WriteConfigFile(cfg, "/tmp/config.cfg")
	if err != nil {
		panic(err)
	}
}
