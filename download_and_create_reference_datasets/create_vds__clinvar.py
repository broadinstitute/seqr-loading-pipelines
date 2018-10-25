#!/usr/bin/env python

from kubernetes.shell_utils import simple_run as run

for genome_version in ('37', '38'):
    run(" ".join([
        "python gcloud_dataproc/run_script.py",
        "--cluster create-clinvar-vds",
        "download_and_create_reference_datasets/hail_scripts/v01/write_clinvar_vds.py",
        "--genome-version {genome_version}"
    ]).format(**locals()))
