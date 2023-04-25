#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.reference_data.combine import join_hts
from hail_scripts.reference_data.config import GCS_PREFIXES
from hail_scripts.utils.hail_utils import write_ht

COMBINED_REFERENCE_HT_PATH = 'combined_reference/combined_reference.GRCh{genome_version}-{version}.ht'
COVERAGE_DATASETS = ['gnomad_genome_coverage', 'gnomad_exome_coverage']
DATASETS = [
    '1kg',
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
]
VERSION = '1.0.0'

def update_existing(destination_path: str, genome_version: str, dataset: str):
    dataset_ht = get_ht(dataset, genome_version)
    destination_ht = hl.read_table(destination_path)
    destination_ht = destination_ht.transmute(**{dataset: dataset_ht[destination_ht.key][dataset]})
    return update_joined_ht_globals(destination_ht, DATASETS, VERSION, COVERAGE_DATASETS, genome_version)

def create_new(genome_version: str):
    return join_hts(
        DATASETS,
        VERSION,
        coverage_datasets=COVERAGE_DATASETS,
        reference_genome=genome_version,
    )

def run(environment: str, genome_version: str, dataset: str):
    destination_path = os.path.join(GCS_PREFIXES[environment], COMBINED_REFERENCE_HT_PATH).format(
        environment=environment,
        genome_version=genome_version,
        version=VERSION,
    )
    if hl.hadoop_exists(os.path.join(destination_path, '_SUCCESS')):
        ht = update_existing(destination_path, dataset, genome_version)
    else:
        ht = create_new(genome_version)
    print(f'Uploading ht to {destination_path}')
    write_ht(ht, destination_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--environment',
        default='dev',
        choices=['dev', 'prod']
    )
    parser.add_argument(
        '--genome-version',
        help='Reference build, 37 or 38',
        choices=['37', '38'],
        default='38'
    )
    parser.add_argument(
        '--dataset',
        choices=DATASETS + COVERAGE_DATASETS,
        required=True,
    )
    args, _ = parser.parse_known_args()
    run(args.environment, args.genome_version)
