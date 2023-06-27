#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import uuid

import hail as hl

from hail_scripts.reference_data.combine import join_hts, update_existing_joined_hts
from hail_scripts.reference_data.config import GCS_PREFIXES, AccessControl
from hail_scripts.utils.hail_utils import write_ht

DATASETS = ['gnomad_non_coding_constraint', 'screen']
INTERVAL_REFERENCE_HT_PATH = 'reference_datasets/interval.ht'


def run(environment: str, dataset: str | None):
    genome_version = '38'
    destination_path = os.path.join(
        GCS_PREFIXES[(environment, AccessControl.PUBLIC)],
        INTERVAL_REFERENCE_HT_PATH,
    ).format(
        genome_version=genome_version,
    )
    if (
        hl.hadoop_exists(os.path.join(destination_path, '_SUCCESS'))
        and dataset is not None
    ):
        ht = update_existing_joined_hts(
            destination_path,
            dataset,
            DATASETS,
            genome_version,
        )
    else:
        ht = join_hts(DATASETS, reference_genome=genome_version)
    ht.describe()
    checkpoint_path = f"{GCS_PREFIXES[('dev', AccessControl.PUBLIC)]}/{uuid.uuid4()}.ht"
    print(f'Checkpointing ht to {checkpoint_path}')
    ht = ht.checkpoint(checkpoint_path, stage_locally=True)
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
        '--dataset',
        choices=DATASETS,
        default=None,
        help='When used, update the passed dataset, otherwise run all datasets.',
    )
    args, _ = parser.parse_known_args()
    run(args.environment, args.dataset)
