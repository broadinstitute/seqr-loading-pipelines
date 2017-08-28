#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("cluster", nargs="?", default="dataproc-cluster-no-vep")
args = p.parse_args()

os.system("gcloud dataproc clusters delete %s" % args.cluster)
