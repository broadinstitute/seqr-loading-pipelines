#!/usr/bin/env python3

import argparse
import os
import random
import sys

from kubernetes.shell_utils import simple_run as run

unique_id = random.randint(10**5, 10**6 - 1)
random_cluster_name = "without-vep-%s" % unique_id

p = argparse.ArgumentParser()
p.add_argument("-c", "--cluster", default=random_cluster_name)
p.add_argument("script")

args, unparsed_args = p.parse_known_args()

cluster_name = args.cluster
script = args.script
script_args = " ".join(['"%s"' % arg for arg in unparsed_args])

os.chdir(os.path.join(os.path.dirname(__file__), "../.."))

run("./gcloud_dataproc/v02/create_cluster_without_VEP.py %(cluster_name)s 2 12" % locals())

if "-h" in sys.argv or "--help" in sys.argv:
    run("python %(script)s -h" % locals())
    sys.exit(0)


run((
    "time ./gcloud_dataproc/submit.py "
    "--hail-version 0.2 "
    "--cluster %(cluster_name)s "
    "%(script)s %(script_args)s") % locals())
