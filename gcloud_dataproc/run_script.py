#!/usr/bin/env python

import argparse
import random
import sys

from hail_scripts.v01.utils.shell_utils import simple_run as run

unique_id = random.randint(10**5, 10**6 - 1)
random_cluster_name = "without-vep-%s" % unique_id

p = argparse.ArgumentParser()
p.add_argument("-p", "--project", default="seqr-project")
p.add_argument("-c", "--cluster", default=random_cluster_name)
p.add_argument("script")

args, unparsed_args = p.parse_known_args()

project = args.project
cluster_name = args.cluster
script = args.script
script_args = " ".join(['"%s"' % arg for arg in unparsed_args])

run((
   "python gcloud_dataproc/create_cluster_without_VEP.py "
   "--project=%(project)s "
   "%(cluster_name)s 2 24") % locals())


if "-h" in sys.argv or "--help" in sys.argv:
    run("python %(script)s -h" % locals())
    sys.exit(0)


run((
    "time ./gcloud_dataproc/submit.py "
    "--cluster %(cluster_name)s "
    "--project %(project)s "
    "%(script)s %(script_args)s") % locals())
