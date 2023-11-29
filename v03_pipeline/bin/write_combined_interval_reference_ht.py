#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

import hail as hl

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path
from v03_pipeline.lib.reference_data.combine import join_hts, update_existing_joined_hts


def run(dataset_type: DatasetType, dataset: str | None):
    reference_genome = ReferenceGenome.GRCh38
    destination_path = valid_reference_dataset_collection_path(
        reference_genome,
        dataset_type,
        ReferenceDatasetCollection.INTERVAL,
    )
    if (
        hl.hadoop_exists(os.path.join(destination_path, '_SUCCESS'))
        and dataset is not None
    ):
        ht = update_existing_joined_hts(
            destination_path,
            dataset,
            reference_genome,
            dataset_type,
            ReferenceDatasetCollection.INTERVAL,
        )
    else:
        ht = join_hts(
            reference_genome,
            dataset_type,
            ReferenceDatasetCollection.INTERVAL,
        )

    ht.describe()
    print(f'Uploading ht to {destination_path}')
    write(ht, destination_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dataset-type',
        type=DatasetType,
        choices=list(DatasetType),
        default=None,
        help='When used, update the passed dataset, otherwise run all datasets.',
    )
    parser.add_argument(
        '--dataset',
        default=None,
        help='When used, update the passed dataset, otherwise run all datasets.',
    )
    args, _ = parser.parse_known_args()
    if args.dataset and args.dataset not in ReferenceDatasetCollection.datasets(
        args.dataset_type,
    ):
        msg = f'{args.dataset} is not a valid dataset for {DatasetType}'
        raise ValueError(msg)
    run(args.dataset_type, args.dataset)
