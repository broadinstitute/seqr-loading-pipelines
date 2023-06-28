#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

import hail as hl

from hail_scripts.reference_data.combine import join_hts, update_existing_joined_hts

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import Env, ReferenceDatasetCollection, ReferenceGenome
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path


def run(env: Env, dataset: str | None):
    reference_genome = ReferenceGenome.GRCh38
    destination_path = valid_reference_dataset_collection_path(
        env,
        reference_genome,
        ReferenceDatasetCollection.INTERVAL,
    )
    if (
        hl.hadoop_exists(os.path.join(destination_path, '_SUCCESS'))
        and dataset is not None
    ):
        ht = update_existing_joined_hts(
            destination_path,
            dataset,
            ReferenceDatasetCollection.INTERVAL.datasets,
            reference_genome.v02_value,
        )
    else:
        ht = join_hts(
            ReferenceDatasetCollection.INTERVAL.datasets,
            reference_genome=reference_genome.v02_value,
        )

    ht.describe()
    print(f'Uploading ht to {destination_path}')
    write(env, ht, destination_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--env',
        type=Env,
        choices=[Env.PROD, Env.DEV],
        default=Env.DEV,
    )
    parser.add_argument(
        '--dataset',
        choices=ReferenceDatasetCollection.INTERVAL.datasets,
        default=None,
        help='When used, update the passed dataset, otherwise run all datasets.',
    )
    args, _ = parser.parse_known_args()
    run(args.env, args.dataset)
