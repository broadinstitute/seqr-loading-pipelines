#!/usr/bin/env python

import argparse
import os
import time
from gcloud_dataproc.utils.machine_types import get_cost

p = argparse.ArgumentParser()
p.add_argument("cluster", nargs="?", default="dataproc-cluster-no-vep")
p.add_argument("num_workers", type=int)
p.add_argument("num_preemptible_workers", type=int)
args = p.parse_args()

print("Resizing to %d worker nodes, %d preemptible nodes" % (args.num_workers, args.num_preemptible_workers))

for machine_type in ["n1-highmem-2", "n1-highmem-4", "n1-highmem-8"]:
    cost1 = get_cost(machine_type=machine_type, hours=1, is_preemptible=False) * args.num_workers
    cost2 = get_cost(machine_type=machine_type, hours=1, is_preemptible=True) * args.num_preemptible_workers
    print("%s: $$$ cost: $%0.2f/h for %s + $%0.2f for %s preemptible/h = $%0.2f / hour total" % (
        machine_type, cost1, args.num_workers, cost2, args.num_preemptible_workers, cost1+cost2))

time.sleep(5)

cluster = args.cluster
num_workers_arg = "--num-workers %(num_workers)s" % args.__dict__ if args.num_workers else ""
num_preemptible_workers_arg = "--num-preemptible-workers %(num_preemptible_workers)s" % args.__dict__ if args.num_preemptible_workers else ""

if not num_workers_arg and not num_preemptible_workers_arg:
    p.exit("At least one of these args must be set: --num-workers, --num-preemptible-workers")

command = """gcloud dataproc clusters update %(cluster)s %(num_workers_arg)s %(num_preemptible_workers_arg)s""" % locals()

print(command)
os.system(command)
