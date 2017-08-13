#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("--project")
p.add_argument("--cluster")
p.add_argument("job_id")
args = p.parse_args()


os.system(" ".join([
    "gcloud dataproc jobs kill",
    "--project=%s" % args.project if args.project else "",
    "--cluster=%s" % args.cluster if args.cluster else "",
    args.job_id,
]))

