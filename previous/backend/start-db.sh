#!/bin/bash

$HOME/solr-6.4.0/bin/solr start -c -s /data/seqr/solr -m 4g

$HOME/apache-cassandra-3.9/bin/cassandra
