#!/usr/bin/env python

from kubernetes.shell_utils import simple_run as run

for genome_version in ('37', '38'):
    run(" ".join([
        "python gcloud_dataproc/v01/run_script.py",
        "--cluster create-clinvar-vds",
        "download_and_create_reference_datasets/v01/hail_scripts/write_clinvar_vds.py",
        "--genome-version {genome_version}"
    ]).format(**locals()))
