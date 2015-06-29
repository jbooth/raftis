package redis

import (
	"errors"
	"io"
)

var (
	ErrMethodNotSupported   = NewError("Method is not supported")
	ErrNotEnoughArgs        = NewError("Not enough arguments for the command")
	ErrTooMuchArgs          = NewError("Too many arguments for the command")
	ErrWrongArgsNumber      = NewError("Wrong number of arguments")
	ErrExpectInteger        = NewError("Expected integer")
	ErrExpectPositivInteger = NewError("Expected positive integer")
	ErrExpectMorePair       = NewError("Expected at least one key val pair")
	ErrExpectEvenPair       = NewError("Got uneven number of key val pairs")
)

var (
	ErrParseTimeout = errors.New("timeout is not an integer or out of range")
)

type ErrorReply struct {
	Code    string
	Message string
}

func (er *ErrorReply) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte("-" + er.Code + " " + er.Message + "\r\n"))
	return int64(n), err
}

func (er *ErrorReply) Error() string {
	return "-" + er.Code + " " + er.Message + "\r\n"
}

func NewError(message string) *ErrorReply {
	return &ErrorReply{Code: "ERROR", Message: message}
}

type FatalReply struct {
	*ErrorReply
}

func NewFatal(message string) *FatalReply {
	return &FatalReply{NewError(message)}
}
