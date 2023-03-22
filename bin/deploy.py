#!/usr/bin/env python3

import argparse
from build.__main__ import main as build
import glob
import itertools
import os
import tempfile
from typing import Iterator, Tuple

from google.cloud import storage
import tomli

PYSCRIPTS_ZIP = "pyscripts.zip"

def parse_version() -> str:
    with open('pyproject.toml', mode='rb') as config:
        toml_file = tomli.load(config)
        return toml_file['project']['version']

def find_files_for_upload(directory: str, gcs_prefix: str, suffix: str, forced_remote_file_name: str = None) -> Iterator[Tuple[str, str]]:
    local_files = glob.iglob(os.path.join(directory, suffix))
    for local_file in local_files:
        rel_path = os.path.relpath(local_file, directory)
        remote_file = os.path.join(gcs_prefix, forced_remote_file_name if forced_remote_file_name else rel_path)
        yield local_file, remote_file

def main(
    bin_directory: str, gcs_project: str, gcs_bucket_name: str, gcs_prefix: str,
) -> None:
    # Build the module
    # This is equivalent to `python -m build --outdir` from the command line
    dist = tempfile.TemporaryDirectory()
    build(['--outdir', f'{dist.name}'])

    storage_client = storage.Client(project=gcs_project)
    bucket = storage_client.bucket(gcs_bucket_name)
    for local_file, remote_file in itertools.chain(
        find_files_for_upload(bin_directory, gcs_prefix, "seqr*.py"),
        find_files_for_upload(dist.name, gcs_prefix, "*.whl", forced_remote_file_name=PYSCRIPTS_ZIP),
    ):
        blob = bucket.blob(remote_file)
        blob.upload_from_filename(local_file)
        print(f"File {local_file} uploaded to {gcs_bucket_name}/{remote_file}.")
  
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bin-directory",
        default="luigi_pipeline",
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
        default=f"releases/optimized-v{parse_version()}",
        help="Prefix within the deployment bucket"
    )
    args = parser.parse_args()
    main(
        args.bin_directory,
        args.gcs_project,
        args.gcs_bucket_name,
        args.gcs_prefix,
    )
