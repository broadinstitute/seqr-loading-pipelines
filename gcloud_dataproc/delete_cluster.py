#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("cluster", nargs="+")
args = p.parse_args()

for cluster in args.cluster:
    os.system("(echo y | gcloud dataproc clusters delete %s >& /dev/null) &" % cluster)
    print("Deleting %s" % cluster)
