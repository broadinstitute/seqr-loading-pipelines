#!/usr/bin/env python3
import argparse
import os

from hail_scripts.reference_data.combine import get_ht
from hail_scripts.reference_data.config import GCS_PREFIXES, AccessControl
from hail_scripts.utils.hail_utils import write_ht

from v03_pipeline.lib.model import Env, ReferenceGenome

HGMD_HT_PATH = 'reference_datasets/hgmd.ht'
PARTITIONS = 100


def run(env: Env, reference_genome: ReferenceGenome):
    dataset = 'hgmd'
    destination_path = os.path.join(
        GCS_PREFIXES[(env.value, AccessControl.PRIVATE)],
        HGMD_HT_PATH,
    ).format(
        genome_version=reference_genome.v02_value,
    )
    ht = get_ht(dataset, reference_genome.v02_value)
    print(f'Uploading ht to {destination_path}')
    write_ht(ht, destination_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--env',
        type=Env,
        choices=list(Env),
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
