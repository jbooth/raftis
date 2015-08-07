package raftis

import (
	"github.com/jbooth/raftis/config"
	log "github.com/jbooth/raftis/rlog"
	"os"
	"sync"
	"syscall"
	"time"
)

type StatsCounter struct {
	l               *sync.Mutex
	serverStartTime int64
	diskTotal       uint64
	currInterval    *config.StatsInterval
	ticker          *time.Ticker
}

// creates new ServerStats
func ServerStats(tickerInterval time.Duration, lg *log.Logger) *StatsCounter {
	ret := &StatsCounter{
		currInterval:    NewStatsInterval(),
		l:               &sync.Mutex{},
		ticker:          time.NewTicker(tickerInterval),
		diskTotal:       totalDiskSpace(),
		serverStartTime: time.Now().Unix(),
	}
	go func() {
		for t := range ret.ticker.C {
			collected := ret.collectInterval()
			lg.Printf("Collected stats interval %s on %s", collected.String(), t.String())
		}
	}()
	return ret
}

func (s *StatsCounter) incrNumReads() {
	s.l.Lock()
	defer s.l.Unlock()
	s.currInterval.NumReads = s.currInterval.NumReads + 1
}

func (s *StatsCounter) incrNumWrites() {
	s.l.Lock()
	defer s.l.Unlock()
	s.currInterval.NumWrites = s.currInterval.NumWrites + 1
}

func (s *StatsCounter) incrNumForwards() {
	s.l.Lock()
	defer s.l.Unlock()
	s.currInterval.NumForwards = s.currInterval.NumForwards + 1
}

// resets current interval to new interval and returns the old interval
func (s *StatsCounter) collectInterval() *config.StatsInterval {
	//todo: this should write to db, but this is defered for now, we just return current interval
	s.l.Lock()
	defer s.l.Unlock()
	s.currInterval.EndTime = time.Now().Unix()
	ret := s.currInterval
	s.currInterval = NewStatsInterval()
	return ret
}

// returns free disk space in bytes
func freeDiskSpace() uint64 {
	var stat syscall.Statfs_t
	wd, _ := os.Getwd()
	syscall.Statfs(wd, &stat)
	// Available blocks * size per block = available space in bytes
	return stat.Bfree * uint64(stat.Bsize)
}

func totalDiskSpace() uint64 {
	var stat syscall.Statfs_t
	wd, _ := os.Getwd()
	syscall.Statfs(wd, &stat)
	// Available blocks * size per block = available space in bytes
	return stat.Blocks * uint64(stat.Bsize)
}

// creates new StatsInterval with start time set to Now
func NewStatsInterval() *config.StatsInterval {
	return &config.StatsInterval{
		StartTime:     time.Now().Unix(),
		DiskSpaceFree: freeDiskSpace(),
	}
}
