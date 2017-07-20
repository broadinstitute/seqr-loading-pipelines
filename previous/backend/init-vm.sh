#!/bin/bash

ROOT="$(cd "`dirname "$0"`"; pwd)"
WHOAMI=`whoami`

# run from $HOME
cd $HOME

# mount ssd
sudo mkdir /data
sudo mkfs.ext4 -F /dev/disk/by-id/google-local-ssd-0
sudo mount -o discard,defaults /dev/disk/by-id/google-local-ssd-0 /data

sudo mkdir /data/seqr
sudo chown $WHOAMI.$WHOAMI /data/seqr

# install dependencies
sudo apt update

sudo apt -y install git build-essential openjdk-8-jdk zip g++ make cmake emacs-nox

# install Hail
git clone https://github.com/cseed/hail.git
(cd hail && git checkout seqrvds && ./gradlew installDist shadowJar hailZip)

# install Apache tools
gsutil -m cp gs://seqr-hail/software/solr-6.4.0.tgz gs://seqr-hail/software/apache-cassandra-3.9-bin.tar.gz gs://seqr-hail/software/spark-2.0.2-bin-hadoop2.7.tgz .

tar xzf solr-6.4.0.tgz
tar xzf apache-cassandra-3.9-bin.tar.gz
tar xzf spark-2.0.2-bin-hadoop2.7.tgz

# backup orig cassandra.yaml
cp apache-cassandra-3.9/conf/cassandra.yaml apache-cassandra-3.9/conf/cassandra.yaml.orig
patch -p0 < $ROOT/cassandra-3.9.yaml.diff

cp -a solr-6.4.0/server/solr /data/seqr
mkdir /data/seqr/cassandra
mkdir /data/seqr/datasets

cat $ROOT/bashrc-hail >> $HOME/.bashrc 
