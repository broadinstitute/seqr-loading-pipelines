#!/usr/bin/env python3
import argparse
import os

from hail_scripts.reference_data.combine import (
    get_enum_select_fields,
    get_select_fields,
    update_joined_ht_globals,
)
from hail_scripts.reference_data.config import CONFIG, GCS_PREFIXES, AccessControl
from hail_scripts.utils.hail_utils import import_vcf, write_ht

HGMD_HT_PATH = 'hgmd/hgmd.GRCh{genome_version}.ht'
PARTITIONS = 100


def run(environment: str, genome_version: str):
    dataset = 'hgmd'
    source_path = CONFIG[dataset][genome_version]['path']
    destination_path = os.path.join(
        GCS_PREFIXES[(environment, AccessControl.PRIVATE)],
        HGMD_HT_PATH,
    ).format(
        genome_version=genome_version,
    )
    mt = import_vcf(
        source_path,
        genome_version=genome_version,
        force=True,
        min_partitions=PARTITIONS,
        skip_invalid_loci=True,
    )
    ht = mt.rows()
    select_fields = {
        **get_select_fields(CONFIG[dataset][genome_version]['select'], ht),
        **get_enum_select_fields(CONFIG[dataset][genome_version]['enum_selects'], ht),
    }
    ht = ht.select(**select_fields)
    ht = update_joined_ht_globals(ht)
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
        default='38',
        choices=['37', '38'],
    )
    args, _ = parser.parse_known_args()
    run(args.environment, args.genome_version)
