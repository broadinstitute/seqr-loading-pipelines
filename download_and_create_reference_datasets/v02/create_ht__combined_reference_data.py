#!/usr/bin/env python3

import argparse
from kubernetes.shell_utils import simple_run as run

parser = argparse.ArgumentParser()
parser.add_argument('-b', '--build', help='Reference build, 37 or 38', choices=["37", "38"], required=True)
args = parser.parse_args()

run((
    "python3 gcloud_dataproc/v02/run_script.py "
    "--cluster create-ht-combined-reference-data "
    "download_and_create_reference_datasets/v02/hail_scripts/write_combined_reference_data_ht.py "
    f"--build {args.build}"))
