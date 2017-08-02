#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("-z", "--zone", default="us-central1-b")
p.add_argument("-m", "--machine-type", default="n1-highmem-4")
p.add_argument("-p", "--project", default="seqr-project")
p.add_argument("cluster", nargs="?", default="dataproc-cluster-vep-grch38")
p.add_argument("num_workers", nargs="?", default="2")
p.add_argument("num_preemtible_workers", nargs="?", default="0")
args = p.parse_args()

# create cluster
command = """gcloud dataproc clusters create %(cluster)s \
    --zone %(zone)s  \
    --master-machine-type %(machine_type)s  \
    --master-boot-disk-size 100  \
    --num-workers %(num_workers)s  \
    --num-preemptible-workers %(num_preemtible_workers)s \
    --project %(project)s \
    --worker-machine-type %(machine_type)s  \
    --worker-boot-disk-size 75 \
    --num-worker-local-ssds 1 \
    --image-version 1.1 \
    --properties "spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,spark:spark.driver.memory=45g,spark:spark.driver.maxResultSize=30g,spark:spark.task.maxFailures=20,spark:spark.yarn.executor.memoryOverhead=30,spark:spark.kryoserializer.buffer.max=1g,hdfs:dfs.replication=1"  \
    --initialization-actions gs://hail-common/hail-init.sh,gs://hail-common/vep/vep/GRCh38/vep85-GRCh38-init.sh
""" % args.__dict__

print(command)
os.system(command)

