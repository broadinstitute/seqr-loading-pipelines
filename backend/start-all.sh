#!/bin/bash

$HOME/solr-6.4.0/bin/solr start -c -s /data/seqr/solr -m 4g

$HOME/apache-cassandra-3.9/bin/cassandra

# seqrd.py logs to logs/seqrd.log
nohup bash -c "python seqrd.py > logs/seqrd.out 2>logs/seqrd.err" &
