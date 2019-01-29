#!/usr/bin/env python

from kubernetes.shell_utils import simple_run as run

run(" ".join([
    "python gcloud_dataproc/v01/run_script.py",
    "--cluster spliceai",
    "download_and_create_reference_datasets/v01/hail_scripts/write_splice_ai_vds.py",
]))
