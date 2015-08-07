#!/bin/sh
set -e

# if these fail run go get first
go install github.com/jbooth/raftis/bin/raftis
go install github.com/jbooth/raftis/bin/genconfig
cp $GOPATH/bin/raftis .
cp $GOPATH/bin/genconfig .
docker build -t raftis/raftis .
docker push raftis/raftis
echo "Published!"
