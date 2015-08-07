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
	debug = true
)

type WrappedLogger struct {
	*log.Logger
}

type Logger struct {
	WrappedLogger
}

func New(out io.Writer, prefix string, flag int, debugLogging bool) *Logger {
	debug = debugLogging
	return &Logger{WrappedLogger{log.New(out, prefix, flag)}}
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.WrappedLogger.Printf(format, v...)
}

func (l *Logger) Printf(format string, v ...interface{}) {
	log.Printf(format, v)
	if debug {
		l.WrappedLogger.Printf(format, v...)
	}
}
