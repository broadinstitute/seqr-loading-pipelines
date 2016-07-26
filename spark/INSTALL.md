0. install Spark in `/local/software/spark-1.6.2-bin-without-hadoop`

0. copy configuration files in `conf/` to `/local/software/spark-1.6.2-bin-without-hadoop`

```
~/seqr-db/spark $ for i in 1 2 3; do scp conf/* seqr-db$i:/local/software/spark-1.6.2-bin-without-hadoop/conf; done
```

0. start Spark on all nodes (run on master/seqr-db1):

```
$ ssh seqr-db1 /local/software/software/spark-1.6.2-bin-without-hadoop/sbin/start-all.sh
```

 - Spark webui:

http://seqr-db1:8080/
