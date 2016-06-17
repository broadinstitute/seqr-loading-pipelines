0. unpack ZooKeeper in `/usr/local/zookeepr-3.4.8`

0. add `conf/zoo.cfg`
 
0. start zookeeper:

`bin/zkServer.sh start`

use

`./bin/zkServer.sh start-foreground`

to start in the foreground logging to the console.

0. add id (1-3) to /local/zookeeper/myid

0. start on all nodes

0. test with ./bin/zkCli.sh:

```
[zkshell: 8] ls /
[zkshell: 9] create /zk_test my_data
[zkshell: 11] ls /
[zkshell: 12] get /zk_test
[zkshell: 14] set /zk_test junk
[zkshell: 15] get /zk_test
[zkshell: 16] delete /zk_test
[zkshell: 17] ls /
```
