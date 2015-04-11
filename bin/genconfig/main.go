package main

import (
	"github.com/jbooth/raftis/config"
	"log"
	"strings"
)

// first arg is MODE, either "singlenode" or "cluster"
// if singlenode, 2nd arg is output directory, 3rd arg is number of shards.  we'll generate 3 datacenters "dc1,dc2,dc3" and a node in each for each shard
// if cluster, 2nd arg is output directory, 3rd arg is a TSV file denoting datacenter,host.
func main() {
	mode := args[0]
	outDir := args[1]

	if strings.ToLower(mode) == "singlenode" {
		numShards := strings.Atoi(args[2])
		cfgs := config.Singlenode(numShards, 3)
		writeConfigs(cfgs)
	} else if strings.ToLower(mode) == "cluster" {

	} else {
		log.Printf("First arg should either be 'singlenode' or 'cluster'.  Args provided : %+v", args)
		log.Printf(`Usage: \
		first arg is MODE, either "singlenode" or "cluster" \
		if singlenode, 2nd arg is output directory, 3rd arg is number of shards.  we'll generate 3 datacenters "dc1,dc2,dc3" and a node in each for each shard \
		if cluster, 2nd arg is output directory, 3rd arg is a TSV file denoting datacenter,host.`)
	}

}

func writeConfigs(cfgs []*config.ClusterConfig, outDir string) error {
	err := os.MkdirAll(outDir)
	if err != nil {
		return err
	}
	for _, cfg := range cfgs {
		outPath := outDir + "/" + cfg.Me.RedisAddr + ".conf"
		err = WriteConfigFile(cfg, outPath)
		if err != nil {
			return err
		}
	}
	return nil
}
