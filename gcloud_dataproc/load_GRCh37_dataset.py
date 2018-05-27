#!/usr/bin/env python

import argparse
import os
import random
import sys

unique_id = random.randint(10**5, 10**6 - 1)
random_cluster_name = "vep-grch37-%s" % unique_id

p = argparse.ArgumentParser()
p.add_argument("-c", "--cluster-name", help="What to use", default=random_cluster_name)
args, unparsed_args = p.parse_known_args()
cluster_name = args.cluster_name


def run(cmd):
    print(cmd)
    os.system(cmd)


if "-h" in sys.argv or "--help" in sys.argv:
    run("python hail_scripts/load_dataset_to_es_pipeline.py -h")
    sys.exit(0)

os.chdir(os.path.join(os.path.dirname(__file__), ".."))

run((
    "python gcloud_dataproc/create_cluster_GRCh37.py "
    "--project=seqr-project "
    "%(cluster_name)s 2 24") % locals())

run((
    "time ./gcloud_dataproc/submit.py "
    "--cluster %(cluster_name)s "
    "--project seqr-project "
    "hail_scripts/load_dataset_to_es_pipeline.py "
    "--genome-version 37 " + " ".join(unparsed_args)
) % locals())


