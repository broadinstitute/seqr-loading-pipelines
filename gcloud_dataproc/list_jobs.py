#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("--project")
p.add_argument("cluster", nargs="?")
args = p.parse_args()

os.system(" ".join([
    "gcloud dataproc jobs list",
    "--project=%s" % args.project if args.project else "",
    "--cluster=%s" % args.cluster if args.cluster else "",
    "| head",
]))
