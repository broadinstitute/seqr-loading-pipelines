#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.computed_fields.vep import (
    CONSEQUENCE_TERM_RANK_LOOKUP,
    get_expr_for_vep_sorted_transcript_consequences_array,
    get_expr_for_worst_transcript_consequence_annotations_struct,
)
from hail_scripts.reference_data.config import GCS_PREFIXES
from hail_scripts.utils.hail_utils import write_ht

VALIDATION_HT_PATH = 'coding_validation/validation.GRCh{genome_version}.{version}.ht'
VERSION = '1.0.0'

def run(environment: str, genome_version: str):
    destination_path = os.path.join(GCS_PREFIXES[environment], VALIDATION_HT_PATH).format(
        genome_version=genome_version,
        version=VERSION,
    )
    print(f'Uploading ht to {destination_path}')
    write_ht(ht, destination_path)

if __name__ == '__main__':
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
        default='38',
    )
    args, _ = parser.parse_known_args()
    run(args.environment, args.genome_version)
