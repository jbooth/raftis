#!/usr/bin/env sh


DIRBASE=/tmp/raftis
function startraftis() {
    dir=$DIRBASE$1
    rm -rf $dir
    mkdir -p $dir

    port=`expr $1 \\* 10 + 16369`
    iport=`expr $1 + 11102`
    echo starting raftis on port $port, iport $iport
    raftis -r 127.0.0.1:$port -i 127.0.0.1:$iport -d $dir -p 127.0.0.1:11103,127.0.0.1:11104,127.0.0.1:11105 >$dir/out.log 2>&1 &
    pid=$!
    echo $pid > $dir/pid
    echo "launched server$1, pid $pid"


}

function stopraftis() {
    kill `cat $DIRBASE$i/pid`
}

function stopall() {
    for i in {1..3};
    do
	pid=`cat $DIRBASE$i/pid`
	kill $pid
	echo "stopped $pid"
    done;

}

trap stopall 2

for i in {1..3};
do
    startraftis $i
done;

for i in {1..3};
do
    pid=`cat $DIRBASE$i/pid`
    wait $pid
    echo "$i down"
    kill $pid
done;