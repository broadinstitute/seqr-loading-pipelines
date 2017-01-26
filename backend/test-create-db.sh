#!/bin/bash

$HOME/solr-6.4.0/bin/solr delete -c test || true

$HOME/solr-6.4.0/bin/solr create_collection -c test -shards 4

$HOME/apache-cassandra-3.9/bin/cqlsh <<EOF
DROP KEYSPACE IF EXISTS test;

CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE test.test (chrom text, start int, ref text, alt text, dataset_5fid text, PRIMARY KEY (chrom, start, ref, alt, dataset_5fid));
EOF
