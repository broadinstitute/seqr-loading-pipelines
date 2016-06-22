Installation instructions
=========================


## Installing from TARBALL

1. Install location 

/local/cassandra

sudo mkdir /local/cassandra

chmod 755 cassandra

2. Got the version 3.7from,

cd cassandra

got URL from,

http://cassandra.apache.org/download/

downloaded,

sudo curl -k -O -L http://apache.claz.org/cassandra/3.7/apache-cassandra-3.7-bin.tar.gz 

3. Extracted,

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


 
 ## Installing as a service
 
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
 

