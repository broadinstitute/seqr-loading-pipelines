#!/bin/bash

$HOME/solr-6.4.0/bin/solr stop || true

PID=`ps auxww | grep CassandraDaemon | grep -v grep | awk '{ print $2 }'`
if [ x"$PID" != x"" ]; then
    echo "killing Cassandra $PID..."
    kill $PID
fi

PID=`ps auxww | grep python | grep 'seqrd.py' | grep -v grep | awk '{ print $2 }'`
if [ x"$PID" != x"" ]; then
    echo "killing seqrd $PID..."
    kill $PID
fi
