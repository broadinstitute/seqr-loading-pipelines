#!/usr/bin/env python

import argparse
import os

from gcloud_dataproc.utils.machine_types import MACHINE_TYPES, get_cost

p = argparse.ArgumentParser()
p.add_argument("-z", "--zone", default="us-central1-b")
p.add_argument("-m", "--machine-type", default="n1-highmem-8", choices=MACHINE_TYPES)
p.add_argument("-p", "--project", default="seqr-project")
p.add_argument("cluster", nargs="?", default="notebook")
p.add_argument("num_workers", nargs="?", help="num worker nodes", default=2, type=int)
p.add_argument("num_preemptible_workers", nargs="?", help="num preemptible worker nodes", default=0, type=int)
args = p.parse_args()

cost1 = get_cost(machine_type=args.machine_type, hours=1, is_preemptible=False) * args.num_workers
cost2 = get_cost(machine_type=args.machine_type, hours=1, is_preemptible=True) * args.num_preemptible_workers
print("$$$ cost: $%0.2f/h + $%0.2f preemptible/h = $%0.2f / hour total" % (cost1, cost2, cost1+cost2))

# create cluster
command = """gcloud beta dataproc clusters create %(cluster)s \
    --max-idle 12h \
    --zone %(zone)s \
    --master-machine-type %(machine_type)s \
    --master-boot-disk-size 100  \
    --num-workers %(num_workers)s \
    --num-preemptible-workers %(num_preemptible_workers)s \
    --project %(project)s \
    --worker-machine-type %(machine_type)s  \
    --worker-boot-disk-size 75 \
    --num-worker-local-ssds 1 \
    --image-version 1.1 \
    --properties "spark:spark.executor.memory=15g,spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,spark:spark.driver.memory=15g,spark:spark.driver.maxResultSize=30g,spark:spark.task.maxFailures=20,spark:spark.yarn.executor.memoryOverhead=15g,spark:spark.kryoserializer.buffer.max=1g,hdfs:dfs.replication=1"  \
    --initialization-actions gs://seqr-hail/init_notebook.py
""" % args.__dict__

#     --network %(project)s-auto-vpc \

print(command)
os.system(command)

command = "python gcloud_dataproc/connect_to_cluster.py --project %(project)s %(cluster)s" % args.__dict__
print(command)
os.system(command)
