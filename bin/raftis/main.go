package main

import (
	"flag"
	"fmt"
	"github.com/jbooth/raftis"
	"github.com/jbooth/raftis/config"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
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

	errChan := make(chan error)
	// spin off server
	go func() {
		errChan <- serve.Serve()
	}()

	// spin off signal handler
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGPIPE, syscall.SIGHUP)
	go func() {
		for {
			s := <-sig
			if s == syscall.SIGPIPE {
				fmt.Println("Got sigpipe!")
			} else {
				// silent quit for other signals
				errChan <- nil
				return
			}
		}
	}()

	// wait for something to come from either signal handler or the mountpoint, unmount and blow up safely
	err = <-errChan
	fmt.Println("Shutting down..")
	if err != nil {
		panic(err)
	}
}
