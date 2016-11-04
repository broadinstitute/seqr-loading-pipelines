0. install Solr in /usr/local/solr-6.0.1

0. install lsof

`sudo yum install lsof`

0. start Solr on each node:

`bin/solr start -c -z localhost:2181`

`-c` is for cloud

`-z` is the ZooKeeper host

`-p 8983` is the default port

0. The Solr Web UI URLs are:

http://seqr-db1:8983/
http://seqr-db2:8983/
http://seqr-db3:8983/
