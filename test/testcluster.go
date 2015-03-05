package raftis

import (
	"fmt"
	"github.com/jbooth/raftis"
	"github.com/xuyu/goredis"
	"math/rand"
	"os"
	"sync"
)

type cluster struct {
	homeDirs []string
	hosts    []raftis.Host
	dbs      []*raftis.Server
	clients  []*goredis.Redis
}

// returns random client
func (c *cluster) rclient() *goredis.Redis {
	nclients := len(c.clients)
	return c.clients[(rand.Int() % nclients)]
}

const debugLogging = true

var testcluster *cluster
var once sync.Once

var shardsForConfig = []raftis.Shard{
	raftis.Shard{
		Slots: []uint32{0, 3, 6, 9},
		Hosts: []raftis.Host{
			raftis.Host{"127.0.0.1:8679", "127.0.0.1:1103", "lvs1"},
			raftis.Host{"127.0.0.1:8689", "127.0.0.1:1104", "lvs1"},
			raftis.Host{"127.0.0.1:8699", "127.0.0.1:1105", "lvs1"},
		},
	},
	raftis.Shard{
		Slots: []uint32{1, 4, 7},
		Hosts: []raftis.Host{
			raftis.Host{"127.0.0.1:8779", "127.0.0.1:1203", "lvs2"},
			raftis.Host{"127.0.0.1:8789", "127.0.0.1:1204", "lvs2"},
			raftis.Host{"127.0.0.1:8799", "127.0.0.1:1205", "lvs2"},
		},
	},
	raftis.Shard{
		Slots: []uint32{2, 5, 8},
		Hosts: []raftis.Host{
			raftis.Host{"127.0.0.1:8879", "127.0.0.1:1303", "lvs3"},
			raftis.Host{"127.0.0.1:8889", "127.0.0.1:1304", "lvs3"},
			raftis.Host{"127.0.0.1:8899", "127.0.0.1:1305", "lvs3"},
		},
	},
}

func setupTest() {

	once.Do(func() {
		os.RemoveAll("/tmp/raftisTest")
		os.MkdirAll("/tmp/raftisTest", os.FileMode(0777))
		testcluster = &cluster{}
		testcluster.homeDirs = []string{
			"/tmp/raftisTest/1", "/tmp/raftisTest/2", "/tmp/raftisTest/3",
			"/tmp/raftisTest/4", "/tmp/raftisTest/5", "/tmp/raftisTest/6",
			"/tmp/raftisTest/7", "/tmp/raftisTest/8", "/tmp/raftisTest/9"}

		fmt.Printf("%+v\n", shardsForConfig)
		testcluster.dbs = make([]*raftis.Server, 9)
		testcluster.hosts = make([]raftis.Host, 9)
		testcluster.clients = make([]*goredis.Redis, 9)
		// crawl the shard nested structure (3 inside 3) to pull our host
		for s := 0; s < 3; s++ {
			for h := 0; h < 3; h++ {
				hostIdx := s*3 + h
				testcluster.hosts[hostIdx] = shardsForConfig[s].Hosts[h]
				fmt.Printf("setting host %d as %+v\n", hostIdx, testcluster.hosts[hostIdx])
			}
		}
		// signal chans to wait for servers to be up
		waitingUp := make([]chan error, 9)
		for i := 0; i < 9; i++ {
			waitingUp[i] = make(chan error)
		}
		// start'em
		for i := 0; i < 9; i++ {
			go func(j int) {
				fmt.Printf("Starting db %d\n", j)
				var err error
				err = os.MkdirAll(testcluster.homeDirs[j], os.FileMode(0777))
				if err != nil {
					panic(err)
				}

				fmt.Printf("host %+v\n", testcluster.hosts[j])

				cfg := &raftis.ClusterConfig{
					NumSlots: uint32(10),
					Me:       testcluster.hosts[j],
					Shards:   shardsForConfig,
				}
				testcluster.dbs[j], err = raftis.NewServer(
          cfg, testcluster.homeDirs[j], debugLogging)

				if err != nil {
					panic(err)
				}
				go testcluster.dbs[j].Serve()
				fmt.Printf("Pushing err to chan for %d\n", j)
				waitingUp[j] <- err
				fmt.Printf("Sent err to chan for %d\n", j)
			}(i)
		}
		// wait all up
		for i := 0; i < 9; i++ {
			fmt.Printf("Waiting on server %d\n", i)
			err := <-waitingUp[i]
			if err != nil {
				fmt.Printf("Error starting server %d : %s", i, err)
				panic(err)
			}
		}
		// setup clients
		var err error
		for i := 0; i < 9; i++ {
			testcluster.clients[i], err = goredis.Dial(&goredis.DialConfig{Address: testcluster.hosts[i].RedisAddr})
			if err != nil {
				panic(err)
			}
		}

		fmt.Printf("All servers up\n")
	})
}
