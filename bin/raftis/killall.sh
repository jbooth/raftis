#!/usr/bin/env sh

ps -eaf | grep raftis | awk '{print $2}' | xargs kill
