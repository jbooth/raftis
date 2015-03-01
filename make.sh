#!/usr/bin/env sh

export GOPATH=Godeps/_workspace:$GOPATH
go build
go install
