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
	homeDirs   []string
	redisAddrs []string
	flotAddrs  []string
	dbs        []*raftis.Server
	clients    []*goredis.Redis
}

// returns random client
func (c *cluster) rclient() *goredis.Redis {
	nclients := len(c.clients)
	return c.clients[(rand.Int() % nclients)]
}

var testcluster *cluster
var once sync.Once

func setupTest() {
	once.Do(func() {
		os.RemoveAll("/tmp/raftisTest")
		os.MkdirAll("/tmp/raftisTest", os.FileMode(0777))
		testcluster = &cluster{}
		testcluster.homeDirs = []string{"/tmp/raftisTest/1", "/tmp/raftisTest/2", "/tmp/raftisTest/3"}
		testcluster.redisAddrs = []string{"localhost:6379", "localhost:6389", "localhost:6399"}
		testcluster.flotAddrs = []string{"localhost:1101", "localhost:1102", "localhost:1103"}

		testcluster.dbs = make([]*raftis.Server, 3)

		// signal chans to wait for servers to be up
		waitingUp := make([]chan error, 3)
		for i := 0; i < 3; i++ {
			waitingUp[i] = make(chan error)
		}

		// start'em
		for i := 0; i < 3; i++ {
			go func(j int) {
				fmt.Printf("Starting db %d\n", j)
				var err error
				err = os.MkdirAll(testcluster.homeDirs[j], os.FileMode(0777))
				if err != nil {
					panic(err)
				}
				testcluster.dbs[j], err = raftis.NewServer(testcluster.redisAddrs[j], testcluster.flotAddrs[j], testcluster.homeDirs[j], testcluster.flotAddrs)
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
		for i := 0; i < 3; i++ {
			fmt.Printf("Waiting on server %d\n", i)
			err := <-waitingUp[i]
			if err != nil {
				fmt.Printf("Error starting server %d : %s", i, err)
				panic(err)
			}
		}
		// setup clients
		testcluster.clients = make([]*goredis.Redis, 3)
		var err error
		for i := 0; i < 3; i++ {
			testcluster.clients[i], err = goredis.Dial(&goredis.DialConfig{Address: testcluster.redisAddrs[i]})
			if err != nil {
				panic(err)
			}
		}

		fmt.Printf("All servers up\n")
	})
}
