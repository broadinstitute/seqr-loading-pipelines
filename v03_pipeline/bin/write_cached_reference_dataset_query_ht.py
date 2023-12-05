#!/usr/bin/env python3
import argparse

import hail as hl

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import (
    valid_cached_reference_dataset_query_path,
    valid_reference_dataset_collection_path,
)
from v03_pipeline.lib.reference_data.config import CONFIG


def get_ht(
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
    query: CachedReferenceDatasetQuery,
) -> hl.Table:
    # If the query is defined over an uncombined reference dataset, use the combiner config.
    if query.reference_dataset:
        config = CONFIG[query.reference_dataset][reference_genome.v02_value]
        return (
            config['custom_import'](config['source_path'], reference_genome)
            if 'custom_import' in config
            else hl.read_table(config['path'])
        )
    return hl.read_table(
        valid_reference_dataset_collection_path(
            reference_genome,
            dataset_type,
            ReferenceDatasetCollection.COMBINED,
        ),
    )


def run(
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
    query: CachedReferenceDatasetQuery,
):
    ht = get_ht(dataset_type, reference_genome, query)
    ht = query.query(ht, dataset_type=dataset_type, reference_genome=reference_genome)
    destination_path = valid_cached_reference_dataset_query_path(
        reference_genome,
        dataset_type,
        query,
    )
    print(f'Uploading ht to {destination_path}')
    write(ht, destination_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--reference-genome',
        type=ReferenceGenome,
        choices=list(ReferenceGenome),
        default=ReferenceGenome.GRCh38,
    )
    parser.add_argument(
        '--dataset-type',
        type=DatasetType,
        choices=list(DatasetType),
        default=None,
        help='When used, update the passed dataset, otherwise run all datasets.',
    )
    parser.add_argument(
        '--query',
        type=CachedReferenceDatasetQuery,
        choices=list(CachedReferenceDatasetQuery),
        required=True,
    )
    args, _ = parser.parse_known_args()
    if (
        args.query
        and args.query
        not in CachedReferenceDatasetQuery.for_reference_genome_dataset_type(
            args.reference_genome,
            args.dataset_type,
        )
    ):
        msg = f'{args.query} is not a valid query for {DatasetType}'
        raise ValueError(msg)
    run(args.dataset_type, args.reference_genome, args.query)
