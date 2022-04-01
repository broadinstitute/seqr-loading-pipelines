#!/usr/bin/env python3

import argparse
import os

from gcloud_dataproc.utils.machine_types import MACHINE_TYPES, get_cost

def create_cluster(cluster='without-vep', machine_type='n1-highmem-8', max_idle='30m', num_workers=2, num_preemptible_workers=0, region=None):
    cost1 = get_cost(machine_type=machine_type, hours=1, is_preemptible=False) * num_workers
    cost2 = get_cost(machine_type=machine_type, hours=1, is_preemptible=True) * num_preemptible_workers
    print("Cost: $%0.2f/h + $%0.2f preemptible/h = $%s / hour" % (cost1, cost2, cost1 + cost2))

    # create cluster
    command = f"""gcloud beta dataproc clusters create {cluster} \
        {f'--region={region}' if region else ''} \
        --max-idle={max_idle} \
        --master-machine-type={machine_type}  \
        --master-boot-disk-size=100GB \
        --num-workers={num_workers} \
        --num-secondary-workers={num_preemptible_workers} \
        --secondary-worker-boot-disk-size=40GB \
        --worker-machine-type={machine_type} \
        --worker-boot-disk-size=40GB \
        --image-version=2.0.29-debian10 \
        --metadata=WHEEL=gs://hail-common/hailctl/dataproc/0.2.85/hail-0.2.85-py3-none-any.whl,PKGS=aiohttp==3.7.4\|aiohttp_session==2.7.0\|asyncinit==0.2.4\|avro==1.10.2\|bokeh==1.4.0\|boto3==1.21.28\|botocore==1.24.28\|decorator==4.4.2\|Deprecated==1.2.12\|dill==0.3.3\|gcsfs==2021.11.1\|google-auth==1.27.0\|google-cloud-storage==1.25.0\|humanize==1.0.0\|hurry.filesize==0.9\|janus==0.6.2\|nest_asyncio==1.5.4\|numpy==1.20.1\|orjson==3.6.4\|pandas==1.3.5\|parsimonious==0.8.1\|plotly==5.5.0\|PyJWT\|python-json-logger==0.1.11\|requests==2.25.1\|scipy==1.6.1\|sortedcontainers==2.1.0\|tabulate==0.8.3\|tqdm==4.42.1\|uvloop==0.16.0\|luigi\|google-api-python-client\|httplib2==0.19.1\|pyparsing==2.4.7 \
        --properties=dataproc:dataproc.cluster-ttl.consider-yarn-activity=false,spark:spark.driver.memory=41g,spark:spark.driver.maxResultSize=0,spark:spark.task.maxFailures=20,spark:spark.kryoserializer.buffer.max=1g,spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,hdfs:dfs.replication=1 \
        --initialization-actions=gs://hail-common/hailctl/dataproc/0.2.85/init_notebook.py
    """

    print(command)
    os.system(command)

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument("-m", "--machine-type", default="n1-highmem-8", choices=MACHINE_TYPES)
    p.add_argument("--max-idle", help="The duration before cluster is auto-deleted after last job completes, such as 30m, 2h or 1d", default="30m")
    p.add_argument("cluster", nargs="?", default="without-vep")
    p.add_argument("num_workers", nargs="?", help="num worker nodes", default=2, type=int)
    p.add_argument("num_preemptible_workers", nargs="?", help="num preemptible worker nodes", default=0, type=int)
    p.add_argument("--region")
    args = p.parse_args()

    create_cluster(
        cluster=args.cluster, machine_type=args.machine_type, max_idle=args.max_idle, num_workers=args.num_workers,
        num_preemptible_workers=args.num_preemptible_workers, region=args.region)

