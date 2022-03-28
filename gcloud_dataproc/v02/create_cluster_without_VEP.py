#!/usr/bin/env python3

import argparse
import os

from gcloud_dataproc.utils.machine_types import MACHINE_TYPES, get_cost

p = argparse.ArgumentParser()
p.add_argument("-m", "--machine-type", default="n1-highmem-8", choices=MACHINE_TYPES)
p.add_argument("--max-idle", help="The duration before cluster is auto-deleted after last job completes, such as 30m, 2h or 1d", default="30m")
p.add_argument("--region")
p.add_argument("cluster", nargs="?", default="without-vep")
p.add_argument("num_workers", nargs="?", help="num worker nodes", default=2, type=int)
p.add_argument("num_preemptible_workers", nargs="?", help="num preemptible worker nodes", default=0, type=int)
args = p.parse_args()

cost1 = get_cost(machine_type=args.machine_type, hours=1, is_preemptible=False) * args.num_workers
cost2 = get_cost(machine_type=args.machine_type, hours=1, is_preemptible=True) * args.num_preemptible_workers
print("Cost: $%0.2f/h + $%0.2f preemptible/h = $%s / hour" % (cost1, cost2, cost1+cost2))

# create cluster
command = """gcloud beta dataproc clusters create %(cluster)s \
    --region=%(region)s \
    --max-idle=%(max_idle)s \
    --master-machine-type=%(machine_type)s  \
    --master-boot-disk-size=100GB \
    --num-workers=%(num_workers)s \
    --num-secondary-workers=%(num_preemptible_workers)s \
    --secondary-worker-boot-disk-size=40GB \
    --worker-machine-type=%(machine_type)s \
    --worker-boot-disk-size=40GB \
    --image-version=2.0.29-debian10 \
    --metadata=WHEEL=gs://hail-common/hailctl/dataproc/0.2.85/hail-0.2.85-py3-none-any.whl,PKGS=aiohttp==3.7.4\|aiohttp_session==2.7.0\|asyncinit==0.2.4\|avro==1.10.2\|bokeh==1.4.0\|boto3==1.21.28\|botocore==1.24.28\|decorator==4.4.2\|Deprecated==1.2.12\|dill==0.3.3\|gcsfs==2021.11.1\|google-auth==1.27.0\|google-cloud-storage==1.25.0\|humanize==1.0.0\|hurry.filesize==0.9\|janus==0.6.2\|nest_asyncio==1.5.4\|numpy==1.20.1\|orjson==3.6.4\|pandas==1.3.5\|parsimonious==0.8.1\|plotly==5.5.0\|PyJWT\|python-json-logger==0.1.11\|requests==2.25.1\|scipy==1.6.1\|sortedcontainers==2.1.0\|tabulate==0.8.3\|tqdm==4.42.1\|uvloop==0.16.0\|luigi\|google-api-python-client\|httplib2==0.19.1\|pyparsing==2.4.7 \
    --properties=spark:spark.driver.memory=41g,spark:spark.driver.maxResultSize=0,spark:spark.task.maxFailures=20,spark:spark.kryoserializer.buffer.max=1g,spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,hdfs:dfs.replication=1 \
    --initialization-actions=gs://hail-common/hailctl/dataproc/0.2.85/init_notebook.py
""" % args.__dict__

print(command)
os.system(command)
