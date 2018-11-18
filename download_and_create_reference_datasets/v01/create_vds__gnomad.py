#!/usr/bin/env python

import argparse
from kubernetes.shell_utils import simple_run as run

genome_versions = ['37', '38']

p = argparse.ArgumentParser()
args, unparsed_args = p.parse_known_args()

script_args = " ".join(['"%s"' % arg for arg in unparsed_args])

run(" ".join([
    "python gcloud_dataproc/v01/run_script.py",
    "--cluster gnomad",
    "download_and_create_reference_datasets/v01/hail_scripts/v01/write_gnomad_vds.py",
    "{script_args}",
]).format(**locals()))
