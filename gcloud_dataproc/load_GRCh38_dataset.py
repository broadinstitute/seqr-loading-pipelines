#!/usr/bin/env python

import argparse
import os
import random
import sys

from gcloud_dataproc.utils import seqr_api


def run(cmd):
    print(cmd)
    os.system(cmd)


if "-h" in sys.argv or "--help" in sys.argv:
    run("python hail_scripts/load_dataset_to_es_pipeline.py -h")
    sys.exit(0)


unique_id = random.randint(10**5, 10**6 - 1)
random_cluster_name = "vep-grch38-%s" % unique_id

p = argparse.ArgumentParser()
p.add_argument("-c", "--cluster-name", help="dataproc cluster name", default=random_cluster_name)
p.add_argument("--seqr-url", help="seqr url for retrieving pedigree info", default="https://seqr.broadinstitute.org")
p.add_argument("--seqr-username", help="seqr username for retrieving pedigree info")
p.add_argument("--seqr-password", help="seqr password for retrieving pedigree info")
p.add_argument("--project-guid", help="seqr project guid")
args, unparsed_args = p.parse_known_args()

os.chdir(os.path.join(os.path.dirname(__file__), ".."))

if args.project_guid:
    unparsed_args += ["--project-guid", args.project_guid]

    seqr_api.download_pedigree_info(args.project_guid, unparsed_args, seqr_url=args.seqr_url)


cluster_name = args.cluster_name

run((
    "python ./gcloud_dataproc/create_cluster_GRCh38.py "
    "--project=seqr-project "
    "%(cluster_name)s 2 24") % locals())

run((
    "time ./gcloud_dataproc/submit.py "
    "--cluster %(cluster_name)s "
    "--project seqr-project "
    "hail_scripts/load_dataset_to_es_pipeline.py "
    "--genome-version 38 " + " ".join(unparsed_args)
) % locals())


