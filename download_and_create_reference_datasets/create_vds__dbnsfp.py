#!/usr/bin/env python

from hail_scripts.v01.utils.shell_utils import simple_run as run

run(" ".join([
    "python gcloud_dataproc/run_script.py",
    "--cluster dbnsfp",
    "download_and_create_reference_datasets/hail_scripts/v01/write_dbnsfp_vds.py",
]))
