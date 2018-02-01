#!/usr/bin/env python

import argparse
import os
import random
import sys

unique_id = random.randint(10**5, 10**6 - 1)
random_cluster_name = "vep-grch38-%s" % unique_id

p = argparse.ArgumentParser()
p.add_argument("-c", "--cluster-name", help="What to use", default=random_cluster_name)
args, unparsed_args = p.parse_known_args()
cluster_name = args.cluster_name


def run(cmd):
    print(cmd)
    os.system(cmd)


if "-h" in sys.argv or "--help" in sys.argv:
    run("python entire_vds_pipeline.py -h")
    sys.exit(0)


run((
    "python create_cluster_GRCh38.py "
    "--project=seqr-project "
    "%(cluster_name)s 2 24") % locals())

run((
    "time ./submit.py "
    "--cluster %(cluster_name)s "
    "--project seqr-project "
    "./entire_vds_pipeline.py "
    "-g 38 " + " ".join(unparsed_args)
) % locals())


