#!/usr/bin/env sh

rm -rf /tmp/raftis1
rm -rf /tmp/raftis2
rm -rf /tmp/raftis3

mkdir -p /tmp/raftis1
mkdir -p /tmp/raftis2
mkdir -p /tmp/raftis3

raftis -r 127.0.0.1:6379 -i 127.0.0.1:1103 -d /tmp/raftis1 -p 127.0.0.1:1103,127.0.0.1:1104,127.0.0.1:1105 >/tmp/raftis1/out.log 2>&1 &
pid1=$!
echo "launched server1, pid $pid1"
raftis -r 127.0.0.1:6389 -i 127.0.0.1:1104 -d /tmp/raftis2 -p 127.0.0.1:1103,127.0.0.1:1104,127.0.0.1:1105 >/tmp/raftis2/out.log 2>&1 &
pid2=$!
echo "launched server2 pid $pid2"
raftis -r 127.0.0.1:6399 -i 127.0.0.1:1105 -d /tmp/raftis3 -p 127.0.0.1:1103,127.0.0.1:1104,127.0.0.1:1105 >/tmp/raftis3/out.log 2>&1 &
pid3=$!
echo "launched server3 pid $pid3"

wait $pid1
echo "$pid1 down"
wait $pid2
echo "$pid2 down"
wait $pid3
echo "$pid3 down"

echo "killing all"
kill $pid1
kill $pid2
kill $pid3
