#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("cluster", nargs="?", default="dataproc-cluster-no-vep")
p.add_argument("num_workers", type=int)
p.add_argument("num_preemtible_workers", type=int)
args = p.parse_args()

cluster =  args.cluster
num_workers_arg = "--num-workers %(num_workers)s" % args.__dict__ if args.num_workers else ""
num_preemptible_workers_arg = "--num-preemptible-workers %(num_preemtible_workers)s" % args.__dict__ if args.num_preemtible_workers else ""

if not num_workers_arg and not num_preemptible_workers_arg:
    p.exit("At least one of these args must be set: --num-workers, --num-preemtible-workers")

command = """gcloud dataproc clusters update %(cluster)s %(num_workers_arg)s %(num_preemptible_workers_arg)s""" % locals()

print(command)
os.system(command)
