package rlog

import (
  "io"
  "log"
)

const (
	Ldate = 1 << iota
	Ltime
  LstdFlags = Ldate | Ltime
  DEBUG = false
)

type Logger struct {
  rlog *log.Logger
}

func New(out io.Writer, prefix string, flag int) *Logger {
  return &Logger{rlog: log.New(out, prefix, flag)}
}


func (l *Logger) Errorf(format string, v ...interface{}) {
  l.rlog.Printf(format, v)
}


func (l *Logger) Printf(format string, v ...interface{}) {
  if DEBUG {
    l.rlog.Printf(format, v)
  }
}
