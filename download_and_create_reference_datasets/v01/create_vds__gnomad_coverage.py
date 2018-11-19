#!/usr/bin/env python

from kubernetes.shell_utils import simple_run as run

run(" ".join([
    "python gcloud_dataproc/v01/run_script.py",
    "--cluster gnomad-coverage",
    "download_and_create_reference_datasets/v01/hail_scripts/v01/write_gnomad_coverage_vds.py",
]))
