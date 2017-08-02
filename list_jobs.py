#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("cluster", default="dataproc-cluster-no-vep")
args = p.parse_args()

os.system("gcloud dataproc jobs list --cluster=%(cluster)s | head" % args.__dict__)
