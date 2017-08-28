#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("job_id")
args = p.parse_args()


os.system(" ".join([
    "gcloud dataproc jobs describe",
    args.job_id,
]))

