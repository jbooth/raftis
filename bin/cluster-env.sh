#!/usr/bin/env sh
LOCAL_TMP=/tmp/raftis-cluster
# REMOTE_RAFTIS_DIR is the directory we'll install the raftis binary & config files to, and store data and logs under
REMOTE_RAFTIS_DIR=/var/raftis
RAFTIS_PORT=8679 # TODO this is currently hardcoded
BACKEND_PORT=1103 # TODO this is currently hardcoded
