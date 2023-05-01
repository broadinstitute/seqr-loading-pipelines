#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.reference_data.combine import join_hts, update_existing_joined_hts
from hail_scripts.reference_data.config import GCS_PREFIXES
from hail_scripts.utils.hail_utils import write_ht

COMBINED_REFERENCE_HT_PATH = 'combined_reference/combined_reference.GRCh{genome_version}-{version}.ht'
DATASETS = [
    'tgp',
    'cadd',
    'dbnsfp',
    'eigen',
    'exac',
    'geno2mp',
    'gnomad_genomes',
    'gnomad_exomes',
    'mpc',
    'primate_ai',
    'splice_ai',
    'topmed',
    'gnomad_genome_coverage',
    'gnomad_exome_coverage',
]
VERSION = '1.0.0'

def run(environment: str, genome_version: str, dataset: str):
    destination_path = os.path.join(GCS_PREFIXES[environment], COMBINED_REFERENCE_HT_PATH).format(
        environment=environment,
        genome_version=genome_version,
        version=VERSION,
    )
    if hl.hadoop_exists(os.path.join(destination_path, '_SUCCESS')):
        ht = update_existing_joined_hts(destination_path, dataset, DATASETS, VERSION, genome_version)
    else:
        ht = join_hts(DATASETS, VERSION, reference_genome=genome_version)
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
        help='Reference build, 37 or 38',
        choices=['37', '38'],
        default='38',
    )
    parser.add_argument(
        '--dataset',
        choices=DATASETS,
        required=True,
    )
    args, _ = parser.parse_known_args()
    run(args.environment, args.genome_version)
