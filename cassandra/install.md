Installation instructions
=========================


# Installing from TARBALL (did the same process with same directory patterns in seqr-db1, seqr-db2 seqr-db3

#Basic installation of Cassandra on nodes (before creating data center)

1-Install location 

/local/cassandra

sudo mkdir /local/cassandra

chmod 755 cassandra

2-Got the version 3.7from,

cd cassandra

got URL from,

http://cassandra.apache.org/download/

downloaded,

sudo curl -k -O -L http://apache.claz.org/cassandra/3.7/apache-cassandra-3.7-bin.tar.gz 

3-Extracted,

sudo tar -xvzf apache-cassandra-3.7-bin.tar.gz

4. go into cassandra install dir,

cd apache-cassandra-3.7

5. Configure installation based on http://wiki.apache.org/cassandra/GettingStarted

i. Didn't have to change  in conf/logback.xml

<file>/var/log/cassandra/system.log</file>

ii. Created directories at the top level cassandra directory (I didn't associate them to the version, should we? I think these transcend version?

commitlog_directory

data_file_directory

saved_caches_directory

iii. Changed permission (logs, so can be 775?),

 sudo chmod 777 *directory
 
iv. Updates conf/cassandra.yaml

# data_file_directories:

#     - /local/cassandra/data_file_directory

# If not set, the default directory is $CASSANDRA_HOME/data/commitlog.

commitlog_directory: /local/cassandra/commitlog_directory

# If not set, the default directory is $CASSANDRA_HOME/data/saved_caches.

saved_caches_directory: /local/cassandra/saved_caches_directory


6. Start cassandra,

[harindra@dmz-seqr-db2 apache-cassandra-3.7]$ sudo ./bin/cassandra -f
Running Cassandra as root user or group is not recommended - please start Cassandra using a different system user.
If you really want to force running Cassandra as root, use -R command line option.

I will start as root for now, but let's make a new user called cassandra?

to start in daemon mode,

sudo ./bin/cassandra -R

NOTE: to kill daemon,

sudo pkill -f CassandraDaemon


7. Test if cassandra is up,


[harindra@dmz-seqr-db2 apache-cassandra-3.7]$ 
[harindra@dmz-seqr-db2 apache-cassandra-3.7]$ ./bin/cqlsh
Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.7 | CQL spec 3.4.2 | Native protocol v4]
Use HELP for help.
cqlsh> 

cqlsh> describe keyspaces;

system_traces  system_schema  system_auth  system  system_distributed




#Converting Cassandra installation on nodes to data center (using https://wiki.apache.org/cassandra/GettingStarted)


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

2- Decide which nodes to use as seeds in Gossip communication.
 
seqr-db1: 69.173.112.35 (seed)
seqr-db2: 69.173.112.36
seqr-db3: 69.173.112.37 (seed)
 
 
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
 

