#!/usr/bin/env python3
import argparse
import os

from hail_scripts.reference_data.combine import get_ht
from hail_scripts.reference_data.config import GCS_PREFIXES, AccessControl
from hail_scripts.utils.hail_utils import write_ht

HGMD_HT_PATH = 'reference_datasets/hgmd.ht'
PARTITIONS = 100


def run(environment: str, genome_version: str):
    dataset = 'hgmd'
    destination_path = os.path.join(
        GCS_PREFIXES[(environment, AccessControl.PRIVATE)],
        HGMD_HT_PATH,
    ).format(
        genome_version=genome_version,
    )
    ht = get_ht(dataset, genome_version)
    print(f'Uploading ht to {destination_path}')
    write_ht(ht, destination_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--environment',
        default='dev',
        choices=['dev', 'prod'],
    )
    parser.add_argument(
        '--genome-version',
        default='38',
        choices=['37', '38'],
    )
    args, _ = parser.parse_known_args()
    run(args.environment, args.genome_version)
