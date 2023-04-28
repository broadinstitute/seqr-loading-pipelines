#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.reference_data.combine import get_enum_select_fields, get_select_fields, update_joined_ht_globals
from hail_scripts.reference_data.config import AccessControl, CONFIG, GCS_PREFIXES
from hail_scripts.utils.hail_utils import import_vcf, write_ht

DATASETS = ['hgmd']
HGMD_HT_PATH = 'hgmd/hgmd.GRCh{genome_version}.ht'
PARTITIONS = 100
VERSION = '1.0.0'

def run(environment: str, genome_version: str):
    dataset = 'hgmd'
    source_path = CONFIG[dataset][genome_version]['path']
    destination_path = os.path.join(GCS_PREFIXES[(environment, AccessControl.PRIVATE)], HGMD_HT_PATH).format(
        environment=environment,
        genome_version=genome_version,
    )
    mt = import_vcf(source_path, genome_version=genome_version, force=True)
    ht = mt.rows().repartition(PARTITIONS)
    select_fields = {
        **get_select_fields(CONFIG[dataset][genome_version]['select'], ht),
        **get_enum_select_fields(CONFIG[dataset][genome_version]['enum_selects'], ht)
    }
    ht = ht.select(**select_fields)
    ht = update_joined_ht_globals(ht, DATASETS, VERSION, [], genome_version)
    print(f'Uploading ht to {destination_path}')
    write_ht(ht, destination_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--environment',
        default='dev',
        choices=['dev', 'prod'],
    )
    parser.add_argument(
        '--genome-version',
        default='37',
        choices=['37', '38'],
    )
    args, _ = parser.parse_known_args()
    run(args.environment, args.genome_version)
