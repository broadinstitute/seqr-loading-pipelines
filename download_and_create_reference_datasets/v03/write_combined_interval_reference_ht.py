#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.reference_data.combine import get_ht, join_hts, update_joined_ht_globals
from hail_scripts.reference_data.config import GCS_PREFIXES, SCREEN_REGION_TYPE_LOOKUP
from hail_scripts.utils.hail_utils import write_ht

DATASETS = ['gnomad_non_coding_constraint', 'screen']
INTERVAL_REFERENCE_HT_PATH = 'combined_interval_reference/combined_interval_reference.GRCh{genome_version}.ht'
VERSION = '1.0.0'

def update_existing(destination_path: str, dataset: str, genome_version: str):
    dataset_ht = get_ht(dataset, genome_version)
    destination_ht = hl.read_table(destination_path)
    destination_ht = destination_ht.transmute(**{dataset: dataset_ht[destination_ht.key][dataset]})
    return update_joined_ht_globals(destination_ht, DATASETS, VERSION, [], genome_version) 

def create_new(genome_version: str):
    return join_hts(
        DATASETS,
        VERSION,
        reference_genome=genome_version,
    )

def run(environment: str):
    genome_version = '38'
    destination_path = os.path.join(GCS_PREFIXES[(environment, 'public')], INTERVAL_REFERENCE_HT_PATH).format(
        environment=environment,
        genome_version=genome_version,
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
        '--dataset',
        choices=DATASETS,
        required=True,
    )
    args, _ = parser.parse_known_args()
    run(args.environment, args.dataset)
