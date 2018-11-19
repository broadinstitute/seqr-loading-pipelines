#!/usr/bin/env python

import argparse
from kubernetes.shell_utils import simple_run as run

genome_versions = ['37', '38']

p = argparse.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=genome_versions)
args, unparsed_args = p.parse_known_args()

script_args = " ".join(['"%s"' % arg for arg in unparsed_args])

cluster_name = 'create-all-reference-data-vds'

if args.genome_version:
    cluster_name += "-grch" + args.genome_version
    genome_versions = [args.genome_version]

for genome_version in genome_versions:
    run(" ".join([
        "python gcloud_dataproc/v01/run_script.py",
        "--cluster {cluster_name}",
        "download_and_create_reference_datasets/v01/hail_scripts/v01/combine_all_variant_level_reference_data.py",
        "--genome-version {genome_version} {script_args}",
    ]).format(**locals()))
