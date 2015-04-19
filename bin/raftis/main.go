package main

import (
	"flag"
	"github.com/jbooth/raftis"
	"github.com/jbooth/raftis/config"
)

var debugLogging bool
var configfile string
var cfg config.ClusterConfig

func init() {
	flag.StringVar(&configfile, "config", "", "config file")
	flag.BoolVar(&debugLogging, "debug", false, "enable debug logging")
	flag.Parse()
}

func main() {
	if configfile == "" {
		panic("Can't go anywhere without a config file")
	}

	cfg, err := config.ReadConfigFile(configfile)
	if err != nil {
		panic(err)
	}

	serve, err := raftis.NewServer(
		cfg,
		debugLogging)

	if err != nil {
		panic(err)
	}
	serve.Serve()
}
