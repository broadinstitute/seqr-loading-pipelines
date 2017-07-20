#!/bin/bash

if [ $# -lt 1 ]; then
    echo 'usage: create-cluster [--vep | --no-vep]    # use --vep if your pyhail script uses hail to run vep. Otherwise, put --no-vep.'
    exit 1
fi

case $1 in
    --vep)
    VEP_ARG='--initialization-actions gs://hail-common/vep/vep/vep81-init.sh'
    ;;
    --no-vep)
    VEP_ARG=
    ;;
    *)
        # unknown option
	echo 'usage: create-cluster [--vep | --no-vep]   # put --vep if your pyhail script uses hail to run vep. Otherwise, put --no-vep.'
	exit 1
    ;;
esac

gcloud dataproc clusters create seqr-${USER}-cluster \
  --zone us-central1-f \
  --master-machine-type n1-highmem-8 \
  --master-boot-disk-size 100 \
  --num-workers 2 \
  --worker-machine-type n1-highmem-8 \
  --worker-boot-disk-size 40 \
  --num-worker-local-ssds 1 \
  --num-preemptible-workers 4 \
  --image-version 1.1  $VEP_ARG \
  --project seqr-project \
  --properties "spark:spark.driver.memory=45g,spark:spark.driver.maxResultSize=50g,spark:spark.task.maxFailures=20,spark:spark.kryoserializer.buffer.max=1g,hdfs:dfs.replication=1"
