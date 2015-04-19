#!/bin/sh

# re-editable template for starting a bunch of servers based on the config files in a directory

ROOTDIR=$1
CONFROOT=${ROOTDIR}/conf
LOGROOT=${ROOTDIR}/log

absConfRoot=`echo $(cd $(dirname "$CONFROOT") && pwd -P)/$(basename "$CONFROOT")`

for c in `ls ${CONFROOT}`
do
  filename=$(basename "$c" | sed "s/.conf$//") 
  echo "starting $filename"
  host=`echo $filename | sed s/\:.*$//`
  port=`echo $filename | sed s/.*\://`
  nohup raftis -config ${absConfRoot}/${c} > ${LOGROOT}/${port}.log 2>&1 < /dev/null &
  echo "nohup raftis -config ${c} > ${LOGROOT}/${port}.log 2>&1 < /dev/null &"
  echo "started $filename pid $!"
done

