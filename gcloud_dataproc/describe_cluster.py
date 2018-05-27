#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("--project")
p.add_argument("cluster")
args = p.parse_args()


os.system(" ".join([
    "gcloud dataproc clusters describe",
    "--project=%s" % args.project if args.project else "",
    args.cluster,
]))

