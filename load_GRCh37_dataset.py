#!/usr/bin/env python

import os
import random
import sys

unique_id = random.randint(10**5, 10**6 - 1)
cluster_name = "vep-grch37-%s" % unique_id


def run(cmd):
    print(cmd)
    os.system(cmd)


if "-h" in sys.argv or "--help" in sys.argv:
    run("python entire_vds_pipeline.py -h")
    sys.exit(0)


run((
    "python create_cluster_GRCh37.py "
    "--project=seqr-project "
    "%(cluster_name)s 2 24") % locals())

run((
    "time ./submit.py "
    "--cluster %(cluster_name)s "
    "--project seqr-project "
    "./entire_vds_pipeline.py "
    "-g 37 " + " ".join(sys.argv[1:])
) % locals())


