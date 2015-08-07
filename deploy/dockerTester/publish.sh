#!/bin/sh
set -e

# if these fail run go get first
docker build -t raftis/tester .
docker push raftis/tester
echo "Published!"
