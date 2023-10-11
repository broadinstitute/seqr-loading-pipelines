#!/usr/bin/env python3
import argparse

import hail as hl

from hail_scripts.reference_data.config import CONFIG

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import (
    valid_cached_reference_dataset_query_path,
    valid_reference_dataset_collection_path,
)


def get_ht(
    reference_genome: ReferenceGenome,
    query: CachedReferenceDatasetQuery,
) -> hl.Table:
    # If the query is defined over an uncombined reference dataset, use the combiner config.
    if query.reference_dataset:
        config = CONFIG[query.reference_dataset][reference_genome.v02_value]
        return (
            config['custom_import'](
                config['source_path'],
                reference_genome.v02_value,
            )
            if 'custom_import' in config
            else hl.read_table(config['path'])
        )
    return hl.read_table(
        valid_reference_dataset_collection_path(
            reference_genome,
            ReferenceDatasetCollection.COMBINED,
        ),
    )


def run(
    reference_genome: ReferenceGenome,
    query: CachedReferenceDatasetQuery,
):
    ht = get_ht(reference_genome, query)
    ht = query.query(ht, reference_genome=reference_genome)
    destination_path = valid_cached_reference_dataset_query_path(
        reference_genome,
        query,
    )
    print(f'Uploading ht to {destination_path}')
    write(ht, destination_path, n_partitions=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--reference-genome',
        type=ReferenceGenome,
        choices=list(ReferenceGenome),
        default=ReferenceGenome.GRCh38,
    )
    parser.add_argument(
        '--query',
        type=CachedReferenceDatasetQuery,
        choices=list(CachedReferenceDatasetQuery),
        required=True,
    )
    args, _ = parser.parse_known_args()
    run(args.reference_genome, args.query)
