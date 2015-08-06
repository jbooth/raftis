#!/bin/bash
set -e

mkdir -p /var/raftis/conf
mkdir -p /var/raftis/data
# generate configs 
/bin/genconfig /var/raftis/conf /var/raftis/data etcd-cluster $MYGROUP $NUMHOSTS http://raftis-dashboard:4001/


# run server
conf=`ls /var/raftis/conf/ | head -1`
/bin/raftis -conf /var/raftis/conf/$conf