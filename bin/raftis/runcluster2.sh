#!/usr/bin/env sh


DIRBASE=/tmp/raftis
function startraftis() {
    dir=$DIRBASE$1
    rm -rf $dir
    mkdir -p $dir

    echo starting raftis $1
    raftis -config local$1.cfg -d $dir >$dir/out.log 2>&1 &
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
