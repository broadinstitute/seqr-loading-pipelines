#!/usr/bin/env python

import argparse
import os

from machine_types import MACHINE_TYPES, get_cost

p = argparse.ArgumentParser()
p.add_argument("-z", "--zone", default="us-central1-b")
p.add_argument("-m", "--machine-type", default="n1-highmem-8", choices=MACHINE_TYPES)
p.add_argument("-p", "--project", default="seqr-project")
p.add_argument("--use-large-machine", action="store_true")
p.add_argument("--max-idle", help="The duration before cluster is auto-deleted after last job completes, such as 30m, 2h or 1d", default="10m")
p.add_argument("cluster", nargs="?", default="vep-grch37")
p.add_argument("num_workers", nargs="?", help="num worker nodes", default=2, type=int)
p.add_argument("num_preemptible_workers", nargs="?", help="num preemptible worker nodes", default=0, type=int)
args = p.parse_args()

cost1 = get_cost(machine_type=args.machine_type, hours=1, is_preemptible=False) * args.num_workers
cost2 = get_cost(machine_type=args.machine_type, hours=1, is_preemptible=True) * args.num_preemptible_workers
print("$$$ cost: $%0.2f/h + $%0.2f preemptible/h = $%0.2f / hour total" % (cost1, cost2, cost1+cost2))

if args.use_large_machine:
    args.master_machine_type = "n1-highmem-16"
else:
    args.master_machine_type = args.machine_type

# create cluster
command = """gcloud beta dataproc clusters create %(cluster)s \
    --max-idle %(max_idle)s \
    --zone %(zone)s  \
    --master-machine-type %(master_machine_type)s  \
    --master-boot-disk-size 100  \
    --num-workers %(num_workers)s  \
    --num-preemptible-workers %(num_preemptible_workers)s \
    --project %(project)s \
    --worker-machine-type %(master_machine_type)s \
    --worker-boot-disk-size 75 \
    --num-worker-local-ssds 1 \
    --image-version 1.1 \
    --properties "spark:spark.executor.memory=15g,spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,spark:spark.driver.memory=15g,spark:spark.driver.maxResultSize=30g,spark:spark.task.maxFailures=20,spark:spark.yarn.executor.memoryOverhead=15g,spark:spark.memory.fraction=0.33,spark:spark.kryoserializer.buffer.max=1g,hdfs:dfs.replication=1" \
    --initialization-actions gs://hail-common/hail-init.sh,gs://hail-common/vep/vep/GRCh37/vep85-GRCh37-init.sh
""" % args.__dict__
#    --network %(project)s-auto-vpc \

print(command)
os.system(command)

