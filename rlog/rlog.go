package rlog

import (
  "io"
  "log"
)

const (
	Ldate = 1 << iota
	Ltime
  LstdFlags = Ldate | Ltime
)

var (
  debug = false
)

type Logger struct {
  rlog *log.Logger
}

func New(out io.Writer, prefix string, flag int, debugLogging bool) *Logger {
  debug = debugLogging
  return &Logger{rlog: log.New(out, prefix, flag)}
}


func (l *Logger) Errorf(format string, v ...interface{}) {
  l.rlog.Printf(format, v)
}


func (l *Logger) Printf(format string, v ...interface{}) {
  if debug {
    l.rlog.Printf(format, v)
  }
}
