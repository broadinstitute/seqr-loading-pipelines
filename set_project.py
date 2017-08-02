#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("project", nargs="?", default="dataproc-cluster-no-vep")
args = p.parse_args()

os.system("gcloud config set project %s" % args.project)
