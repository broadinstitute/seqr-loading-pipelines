#!/usr/bin/env python

from kubernetes.shell_utils import simple_run as run

for genome_version in ["37", "38"]:
    run(" ".join([
        "python gcloud_dataproc/run_script_GRCh38.py",
        "--cluster dataset-validation-kt-38",
        "download_and_create_reference_datasets/hail_scripts/v01/write_dataset_validation_kt.py",
        "--genome-version {}",
    ]).format(genome_version))

