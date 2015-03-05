package main

import (
	"flag"
	"fmt"
	"github.com/jbooth/raftis"
	"os"
	"strings"
)

var hostname string
var redisAddr string
var internalAddr string
var dataDir string
var debugLogging bool
var peers []string

func init() {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		panic(fmt.Errorf("Err getting hostname : %s", err))
	}

	flag.StringVar(&redisAddr, "r",
    fmt.Sprintf("%s:%d", hostname, 8367),
    "addr:port to serve for redis, defaults to 127.0.0.1:8367")

	flag.StringVar(&internalAddr, "i",
    fmt.Sprintf("%s:%d", hostname, 1103),
    "addr:port to serve for internal consensus tracking")

	flag.StringVar(&dataDir, "d", "/tmp/raftis", "data directory")
	var peerStr string

	flag.StringVar(&peerStr, "p",
    "SINGLEHOST",
    "comma-separated list of peers, or 'SINGLEHOST' to run as a 1-node cluster")

  flag.BoolVar(&debugLogging, "debug", false,
    "enable debug logging")

	flag.Parse()

	if strings.ToUpper(peerStr) == "SINGLEHOST" {
		panic("user should implement singlehost code")
	}
	peers = strings.Split(peerStr, ",")
}

func main() {
	serve, err := raftis.NewServer(
		redisAddr,
		internalAddr,
		dataDir,
		peers,
    debugLogging)

	if err != nil { panic(err) }
	serve.Serve()
}
