#!/bin/bash
set -e

mkdir -p /var/raftis/conf
mkdir -p /var/raftis/data
# generate configs 
/bin/genconfig /var/raftis/conf /var/raftis/data etcd-cluster $NUMHOSTS $ETCDURL


# run server
conf=`ls /var/raftis/conf/ | head -1`
/bin/raftis -conf /var/raftis/conf/$conf
