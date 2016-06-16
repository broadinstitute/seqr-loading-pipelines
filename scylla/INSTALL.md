

RHEL 7 install instructions: 

http://www.scylladb.com/doc/getting-started-rpm/


0. ```sudo yum remove -y abrt    # remove conflicting package if it exists```
0. ```sudo yum install epel-release wget   # install pre-reqs```
0. ```sudo wget -O /etc/yum.repos.d/scylla.repo  http://downloads.scylladb.com/rpm/centos/scylla-1.2.repo  # get the scylla repo``` 
0. ```sudo yum install scylla    # install scylla```
0. 
