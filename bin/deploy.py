#!/usr/bin/env python3

import argparse
from build.__main__ import main as build
import glob
import os
import pkg_resources
from typing import Iterator, Tuple

from google.cloud import storage

import luigi_pipeline

def find_pyfiles_for_upload(bin_directory: str, gcs_prefix: str) -> Iterator[Tuple[str, str]]:
    local_files = glob.iglob(os.path.join(dags_directory, "**/*.py"), recursive=True)
    for local_file in local_files:
        if any((pattern in local_file for pattern in EXCLUDE_PATTERNS)):
            continue
        rel_path = os.path.relpath(local_file, dags_directory)
        remote_file = os.path.join(gcs_prefix, rel_path)
        yield local_file, remote_file

def main(
    bin_directory: str, gcs_project: str, gcs_bucket_name: str, gcs_prefix: str,
) -> None:
    # Build the module
    # This is equivalent to `python -m build` from the command line
    build('')    

    storage_client = storage.Client(project=gcs_project)
    bucket = storage_client.bucket(gcs_bucket_name)
    for local_file, remote_file in find_pyfiles_for_upload(bin_directory, gcs_prefix):
        blob = bucket.blob(remote_file)
        #blob.upload_from_filename(local_file)
        print(f"File {local_file} uploaded to {gcs_bucket_name}/{remote_file}.")
  
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bin-directory",
        default="luigi_pipeline/bin",
        help="Relative path to the source directory containing executable mains",
    )
    parser.add_argument(
        "--gcs-project",
        default="seqr-project",
        help="Name of the GCS project",
    )
    parser.add_argument(
        "--gcs-bucket-name",
        default="seqr-luigi",
        help="Name of the deployment bucket without the gs:// prefix",
    )
    parser.add_argument(
    	"--gcs-prefix",
    	default=f"releases/optimized-v{luigi_pipeline.__version__}",
    	help="Prefix within the deployment bucket"
    )
    args = parser.parse_args()
    main(
        args.bin_directory, 
        args.gcs_project,
        args.gcs_bucket_name,
        args.gcs_prefix,
    )
