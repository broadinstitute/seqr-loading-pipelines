#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.reference_data.constants import GCS_PREFIXES
from hail_scripts.reference_data.combine import join_hts
from hail_scripts.utils.hail_utils import write_ht

INTERVAL_REFERENCE_HT_PATH = 'interval_reference/interval_reference.GRCh{genome_version}.{version}.ht'
VERSION = '1.0.0'


def run(environment: str):
	genome_version = '38'
    ht = join_hts(
        ['gnomad_non_coding_constraint', 'screen'], reference_genome="38"
    )
	destination_path = os.path.join(GCS_PREFIXES[environment], INTERVAL_REFERENCE_HT_PATH).format(
        environment=environment,
        genome_version=genome_version,
        version=version,
    )
    print(f'Uploading ht from {hl.eval(ht.sourceFilePath)} to {destination_path}')
    write_ht(ht, destination_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--environment',
        default='dev',
        choices=['dev', 'prod']
    )
    args = parser.parse_args()
    run(args.environment)