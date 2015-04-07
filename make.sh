#!/usr/bin/env sh

export GOPATH=Godeps/_workspace:$GOPATH
cd raftis && go build && go install && cd ..
