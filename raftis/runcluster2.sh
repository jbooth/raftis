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

case $1 in
start)
	echo starting raftis
	for i in {1..3};
	do
	    startraftis $i
	done;
;;
stop)
	echo stopping raftis
	for i in {1..3};
	do
	    pid=`cat $DIRBASE$i/pid`
	    kill $pid
	    echo "killed $pid"
	done;
	for i in {1..3};
	do
	    pid=`cat $DIRBASE$i/pid`
            while kill -0 "$pid"; do
		sleep 0.5
            done
	    echo "$pid returned"
	done;
	echo stopped raftis
;;
*)
	echo "$0 {start|stop}"
;;
esac