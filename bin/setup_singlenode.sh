#!/bin/sh

ROOTDIR=$1
NUMSHARDS=3


echo "Building singlenode config for ${NUMSHARDS} virtual shards at ${ROOTDIR}, make sure genconfig installed via go install github.com/jbooth/raftis/bin/genconfig"
PATH=$GOPATH/bin:$PATH

DATAROOT=${ROOTDIR}/data
CONFROOT=${ROOTDIR}/conf
LOGROOT=${ROOTDIR}/log
mkdir -p ${DATAROOT}
mkdir -p ${CONFROOT}
mkdir -p ${LOGROOT}

genconfig ${CONFROOT} ${DATAROOT} singlenode ${NUMSHARDS}
