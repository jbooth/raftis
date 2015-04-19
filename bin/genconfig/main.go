package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/jbooth/raftis/config"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

// first 3 args are configDir,
// if singlenode, 2nd arg is output directory, 3rd arg is number of shards.  we'll generate 3 datacenters "dc1,dc2,dc3" and a node in each for each shard
// if cluster, 2nd arg is output directory, 3rd arg is a TSV file denoting group,host.
func main() {
	flag.Parse()
	args := flag.Args()
	fmt.Printf("Args : %+v\n", args)
	if len(args) < 4 {
		usage(args)
		return
	}
	configDir := args[0]
	dataDir := args[1]
	mode := args[2]

	fmt.Printf("mode %s\n", mode)

	if strings.ToLower(mode) == "singlenode" {
		numShards, err := strconv.Atoi(args[3])
		if err != nil {
			panic(err)
		}
		cfgs := config.Singlenode(numShards, 3, dataDir)
		err = writeConfigs(cfgs, configDir)
		if err != nil {
			panic(err)
		}
	} else if strings.ToLower(mode) == "cluster" {
		hosts, err := readHosts(args[3])
		if err != nil {
			panic(err)
		}
		dataDirs := make([]string, len(hosts), len(hosts))
		for i, _ := range hosts {
			dataDirs[i] = dataDir
		}
		cfgs := config.AutoCluster(100, hosts, dataDirs)
		err = writeConfigs(cfgs, configDir)
		if err != nil {
			panic(err)
		}
	} else {
		usage(args)
		return
	}

}

func usage(args []string) {
	log.Printf("First arg should either be 'singlenode' or 'cluster'.  Args provided : %+v", args)
	log.Printf(`Usage: \
		first arg is MODE, either "singlenode" or "cluster" \
		if singlenode, 2nd arg is output directory, 3rd arg is number of shards.  we'll generate 3 datacenters "dc1,dc2,dc3" and a node in each for each shard \
		if cluster, 2nd arg is output directory, 3rd arg is a TSV file denoting datacenter,host.`)

}

func writeConfigs(cfgs []config.ClusterConfig, configDir string) error {
	err := os.MkdirAll(configDir, 0777)
	if err != nil {
		return err
	}
	for _, cfg := range cfgs {
		outPath := configDir + "/" + cfg.Me.RedisAddr + ".conf"
		err = config.WriteConfigFile(&cfg, outPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func readHosts(hostPath string) (hosts []config.Host, err error) {
	in, err := os.Open(hostPath)
	if err != nil {
		return nil, err
	}
	bufIn := bufio.NewReader(in)
	// file is group whitespace host
	ret := make([]config.Host, 0, 0)
	for {
		line, err := bufIn.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				return nil, err
			} else {
				break
			}
		} else {

			fmt.Println("parsing line " + line)

			groupHost := strings.Fields(line)
			if len(groupHost) != 2 {
				return nil, fmt.Errorf("Expected 2 fields per line, bad line %s", line)
			}
			group := groupHost[0]
			host := groupHost[1]
			h := config.Host{
				RedisAddr:    fmt.Sprintf("%s:%d", host, 8679),
				FlotillaAddr: fmt.Sprintf("%s:%d", host, 1103),
				Group:        group,
			}
			ret = append(ret, h)
		}
	}
	return ret, nil
}
