package main

import (
	"flag"
	"github.com/jbooth/raftis"
)

var debugLogging bool
var configfile string
var dataDir string
var config raftis.ClusterConfig

func init() {
  flag.StringVar(&configfile, "config", "", "config file")
	flag.StringVar(&dataDir, "d", "/tmp/raftis", "data directory")
  flag.BoolVar(&debugLogging, "debug", false, "enable debug logging")

	flag.Parse()
}

func main() {
  if configfile == "" {
		panic("Can't go anywhere without a config file")
  }

  config, err := raftis.ReadConfigFile(configfile)
  if err != nil { panic(err) }

	serve, err := raftis.NewServer(
    config,
		dataDir,
    debugLogging)

	if err != nil { panic(err) }
	serve.Serve()
}
