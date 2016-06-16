

RHEL 7 install instructions: 

http://www.scylladb.com/doc/getting-started-rpm/

INSTALL:

0. ```sudo yum remove -y abrt    # remove conflicting package if it exists```
0. ```sudo yum install epel-release wget   # install pre-reqs```
0. ```sudo wget -O /etc/yum.repos.d/scylla.repo  http://downloads.scylladb.com/rpm/centos/scylla-1.2.repo  # get the scylla repo``` 
0. ```sudo yum install scylla    # install scylla```
0. ```sudo scylla_setup```  
      Answer `yes` to everything except say `no` to disabling SELinux, and `no` to installing RAID.
      (keep SELinux based on: https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Security-Enhanced_Linux/chap-Security-Enhanced_Linux-Introduction.html#sect-Security-Enhanced_Linux-Introduction-Benefits_of_running_SELinux)

0. ```sudo reboot```


CONFIGURE:

0. edit /etc/scylla/scylla.yaml 

