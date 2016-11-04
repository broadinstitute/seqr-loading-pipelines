Installation instructions
=========================
Basic installation on nodes (before creating data center)  
Repeated on seqr-db1, seqr-db2, seqr-db3  
Based on http://wiki.apache.org/cassandra/GettingStarted    

0. download tarball  
  ```
mkdir /local/software/cassandra
cd /local/software/cassandra  
chmod 755 .  
curl -k -O -L http://apache.claz.org/cassandra/3.7/apache-cassandra-3.7-bin.tar.gz   
tar -xvzf apache-cassandra-3.7-bin.tar.gz  
cd apache-cassandra-3.7
  ```

0. created directories:  
  `mkdir /local/software/cassanrda/commitlog_directory`  
  `mkdir /local/software/cassanrda/data_file_directory`  
  `mkdir /local/software/cassanrda/saved_caches_directory`     
  (I didn't associate them to the version, should we? I think these transcend version?)

0. Updated conf/cassandra.yaml   
    - data_file_directories:  `/local/software/cassandra/data_file_directory` 
    - commitlog_directory: `/local/software/cassandra/commitlog_directory` (if not set, the default directory is $CASSANDRA_HOME/data/commitlog)   
    - saved_caches_directory: `/local/cassandra/saved_caches_directory` (if not set, the default directory is $CASSANDRA_HOME/data/saved_caches)   

0. started cassandra: 
  ```
  # start as root for now, but let's make a new user called cassandra?
  [harindra@dmz-seqr-db2 apache-cassandra-3.7]$ sudo ./bin/cassandra -f   
  ```
  to start in daemon mode:
  ```sudo ./bin/cassandra -R```
  to kill daemon:
  ```sudo pkill -f CassandraDaemon```

0. test if cassandra is up:
  ```
  [harindra@dmz-seqr-db2 apache-cassandra-3.7]$ ./bin/cqlsh
  Connected to Test Cluster at 127.0.0.1:9042.
  [cqlsh 5.0.1 | Cassandra 3.7 | CQL spec 3.4.2 | Native protocol v4]
  Use HELP for help.
  cqlsh> describe keyspaces;
  system_traces  system_schema  system_auth  system  system_distributed
  ```

#Converting Cassandra installation on nodes to data center 
(using https://wiki.apache.org/cassandra/GettingStarted,https://netangels.net/knowledge-base/cassandra-multi-datacenter-setup/)


1-First get IP addresses of the 3 nodes via ifconfig.

For example,

[harindra@dmz-seqr-db1 ~]$ ifconfig
eth0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
        inet 69.173.112.35  netmask 255.255.255.0  broadcast 69.173.112.255
        ether 00:50:56:bb:69:a8  txqueuelen 1000  (Ethernet)
        RX packets 21773314  bytes 103905116842 (96.7 GiB)
        RX errors 0  dropped 24  overruns 0  frame 0
        TX packets 12985667  bytes 75340139504 (70.1 GiB)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0

lo: flags=73<UP,LOOPBACK,RUNNING>  mtu 65536
        inet 127.0.0.1  netmask 255.0.0.0
        loop  txqueuelen 0  (Local Loopback)
        RX packets 12351034  bytes 175447929012 (163.3 GiB)
        RX errors 0  dropped 0  overruns 0  frame 0
        TX packets 12351034  bytes 175447929012 (163.3 GiB)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0

seqr-db1: 69.173.112.35
seqr-db2: 69.173.112.36
seqr-db3: 69.173.112.37 

2-Decide which nodes to use as seeds in Gossip communication. We are using all 3 as seeds
 
seqr-db1: 69.173.112.35 (seed)
seqr-db2: 69.173.112.36 (seed)
seqr-db3: 69.173.112.37 (seed)
 
 
 
3-Update cons/cassandra.yaml with seed node IPs 
seqr-db1:/conf/cassandra.yaml 
          - seeds: "69.173.112.35,69.173.112.36,69.173.112.37"
seqr-db2:/conf/cassandra.yaml 
 		  - seeds: "69.173.112.35,69.173.112.36,69.173.112.37"
seqr-db3:/conf/cassandra.yaml 
 		  - seeds: "69.173.112.35,69.173.112.36,69.173.112.37"
 		  
4-Update listed addresses in all 3
listen_address: IP_OF_INSTANCE
rpc_address: IP_OF_INSTANCE 

seqr-db1:/conf/cassandra.yaml 
	listen_address: 69.173.112.35
	rpc_address: 69.173.112.35
	
seqr-db2:/conf/cassandra.yaml 
	rpc_address: 69.173.112.36
	listen_address: 69.173.112.36
	

seqr-db3:/conf/cassandra.yaml 
	rpc_address: 69.173.112.37
	listen_address: 69.173.112.37
 
4-Update protocol

seqr-db1:
	endpoint_snitch: GossipingPropertyFileSnitch
seqr-db2:
	endpoint_snitch: GossipingPropertyFileSnitch
seqr-db3:
	endpoint_snitch: GossipingPropertyFileSnitch
	
	
	
5-Update cluster name  [NOTE: I backtracked on this temporarily and reverted to as-was, found a bug and found first need to do https://support.datastax.com/hc/en-us/articles/205289825-Change-Cluster-Name-)]

seqr-db1:
	cluster_name: 'seqrdb'
seqr-db2:
	cluster_name: 'seqrdb'	
seqr-db3:
	cluster_name: 'seqrdb'


6-Update conf/cassandra-rackdc.properties  [NOTE: I backtracked on this temporarily and reverted to as-was, found a bug and found first need to do https://support.datastax.com/hc/en-us/articles/205289825-Change-Cluster-Name-)]
seqr-db1	
	dc=US
	rack=RAC1
seqr-db2
	dc=US
	rack=RAC1
seqr-db3
	dc=US
	rack=RAC1
	
	
7-Start servers one by one as earlier,
seqr-db1
	sudo ./bin/cassandra -R
seqr-db2
	sudo ./bin/cassandra -R
seqr-db3
	sudo ./bin/cassandra -R
	
	
8. Test if it is seeing nodes,

[harindra@dmz-seqr-db1 apache-cassandra-3.7]$ ./bin/nodetool status
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens       Owns (effective)  Host ID                               Rack
UN  69.173.112.37  124.81 KiB  256          64.5%             93448f2f-1ef4-45da-a182-643ae5029c26  rack1
UN  69.173.112.36  98.59 KiB  256          69.8%             63cf71c1-53d4-40ba-aa2f-b76775742b0f  rack1
UN  69.173.112.35  136.55 KiB  256          65.7%             17046571-b7de-40c5-a010-f8bebcc23c27  rack1

[harindra@dmz-seqr-db1 apache-cassandra-3.7]$ 






 ## Appendix-A Installing as a service
 
 (http://docs.datastax.com/en/cassandra/3.x/cassandra/install/installRHEL.html)
 
 
 1. Add the repo to yum,
 
  sudo vi /etc/yum.repos.d/datastax.repo
  
  then paste in,
 
[datastax-ddc] 

name = DataStax Repo for Apache Cassandra

baseurl = http://rpm.datastax.com/datastax-ddc/3.7

enabled = 1

gpgcheck = 0 

 
 2. Do yum install,
 
 sudo yum install datastax-ddc
 
 3. Conflict with scylla
 
 s.noarch
......
  file /usr/lib/python2.7/site-packages/cassandra_pylib-0.0.0-py2.7.egg-info from install of datastax-ddc-3.7.0-1.noarch conflicts with file from package scylla-tools-1.2.0-20160614.cfd4575.el7.centos.noarch

Error Summary

AFTER discussion with Ben, Cotton, Monkol we decided to abandon yum based installing and went with tarball. Benefits of tarball

1. Can control where all files get installed
2. Can control installation, config files better
 

