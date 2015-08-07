#!/bin/bash
set -e

mkdir -p /var/raftis/conf
mkdir -p /var/raftis/data
# generate configs 
echo "generating conf"
/bin/genconfig /var/raftis/conf /var/raftis/data etcd-cluster $NUMHOSTS $ETCDBASE $ETCDURL
echo "generated conf!"


# run server
conf=`ls /var/raftis/conf/ | head -1`
echo "running raftis with conf: "
cat /var/raftis/conf/$conf
/bin/raftis -config /var/raftis/conf/$conf
