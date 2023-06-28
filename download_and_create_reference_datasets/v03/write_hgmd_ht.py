#!/usr/bin/env python3
import argparse

from hail_scripts.reference_data.combine import get_ht

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import Env, ReferenceDatasetCollection, ReferenceGenome
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path


def run(env: Env, reference_genome: ReferenceGenome):
    dataset = ReferenceDatasetCollection.HGMD.datasets[0]
    ht = get_ht(dataset, reference_genome.v02_value)
    destination_path = valid_reference_dataset_collection_path(
        env,
        reference_genome,
        ReferenceDatasetCollection.HGMD,
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
    args, _ = parser.parse_known_args()
    run(args.env, args.reference_genome)
