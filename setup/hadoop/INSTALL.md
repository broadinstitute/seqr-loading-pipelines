0. install hadoop in `/usr/local/hadoop-2.7.2`

0. mkdir /local/hdfs

0. add hdfs-site.xml, slaves, core-site.xml, hadoop-env.sh to etc/hadoop

0. format hdfs on seqr-db1/namenode

`./bin/hdfs namenode -format -force`

0. start HDFS (on namenode only):

`sbin/start-dfs.sh`

Top stop HDFS, run `sbin/stop-dfs.sh`.

0. HDFS WebUI can be found at:

http://seqr-db1:50070/
