#!/usr/bin/env python3
import argparse

from hail_scripts.reference_data.config import CONFIG

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import CachedReferenceDatasetQuery, Env, ReferenceGenome
from v03_pipeline.lib.paths import cached_reference_dataset_query_path


def run(
    env: Env,
    reference_genome: ReferenceGenome,
    query: CachedReferenceDatasetQuery,
):
    config = CONFIG[dataset][reference_genome.v02_value]
    ht = (
        config['custom_import'](config['source_path'], reference_genome.v02_value)
        if 'custom_import' in config
        else hl.read_table(config['path'])
    )
    ht = query.query(ht, reference_genome)
    destination_path = cached_reference_dataset_query_path(
        env,
        reference_genome,
        query,
    )
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
        '--reference-genome',
        type=ReferenceGenome,
        choices=list(ReferenceGenome),
        default=ReferenceGenome.GRCh38,
    )
    parser.add_argument(
        '--query',
        type=CachedReferenceDatasetQuery,
        choices=list(CachedReferenceDatasetQuery),
    )
    args, _ = parser.parse_known_args()
    run(args.env, args.reference_genome, args.query)
