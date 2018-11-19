#!/usr/bin/env python3

import argparse
import os

from gcloud_dataproc.utils.machine_types import MACHINE_TYPES, get_cost

p = argparse.ArgumentParser()
p.add_argument("-m", "--machine-type", default="n1-highmem-8", choices=MACHINE_TYPES)
p.add_argument("--use-large-machine", action="store_true")
p.add_argument("--max-idle", help="The duration before cluster is auto-deleted after last job completes, such as 30m, 2h or 1d", default="30m")
p.add_argument("cluster", nargs="?", default="vep-grch38")
p.add_argument("num_workers", nargs="?", help="num worker nodes", default=2, type=int)
p.add_argument("num_preemptible_workers", nargs="?", help="num preemptible worker nodes", default=0, type=int)
args = p.parse_args()

cost1 = get_cost(machine_type=args.machine_type, hours=1, is_preemptible=False) * args.num_workers
cost2 = get_cost(machine_type=args.machine_type, hours=1, is_preemptible=True) * args.num_preemptible_workers
print("$$$ cost: $%0.2f/h + $%0.2f preemptible/h = $%0.2f / hour total" % (cost1, cost2, cost1+cost2))

if args.use_large_machine:
    args.machine_type = "n1-highmem-16"
else:
    args.machine_type = args.machine_type

# create cluster
command = """gcloud beta dataproc clusters create %(cluster)s \
    --max-idle=%(max_idle)s \
    --master-machine-type=%(machine_type)s  \
    --master-boot-disk-size=100GB \
    --num-workers=%(num_workers)s \
    --num-preemptible-workers=%(num_preemptible_workers)s \
    --preemptible-worker-boot-disk-size=40GB \
    --worker-machine-type=%(machine_type)s \
    --worker-boot-disk-size=40GB \
    --image-version=1.2 \
    --metadata=JAR=gs://hail-common/builds/devel/jars/hail-devel-17a988f2a628-Spark-2.2.0.jar,ZIP=gs://hail-common/builds/devel/python/hail-devel-17a988f2a628.zip,MINICONDA_VERSION=4.4.10 \
    --properties=spark:spark.driver.memory=41g,spark:spark.driver.maxResultSize=0,spark:spark.task.maxFailures=20,spark:spark.kryoserializer.buffer.max=1g,spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,hdfs:dfs.replication=1 \
    --initialization-actions=gs://dataproc-initialization-actions/conda/bootstrap-conda.sh,gs://hail-common/vep/vep/GRCh38/vep85-GRCh38-init.sh,gs://hail-common/cloudtools/init_notebook1.py
""" % args.__dict__

#     --network %(project)s-auto-vpc \


print(command)
os.system(command)

