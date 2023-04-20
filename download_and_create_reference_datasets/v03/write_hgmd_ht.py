#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.reference_data.config import GCS_PREFIXES, HGMD_CONFIG
from hail_scripts.utils.hail_utils import write_ht

HGMD_HT_PATH = 'hgmd/hgmd.GRCh{genome_version}.ht'
VERSION = '1.0.0'

HGMD_CLASS_MAPPING = hl.dict(
    hl.enumerate(
        [
            'DFP',
            'DM',
            'DM?',
            'DP', 
            'FP',
            'R',
        ],
        index_first=False
    )
)

def run(environment: str, genome_version: str):
    source_path = HGMD_CONFIG[genome_version]
    destination_path = os.path.join(GCS_PREFIXES[(environment, 'private')], HGMD_HT_PATH).format(
        environment=environment,
        genome_version=genome_version,
    )
    mt = hl.import_vcf(source_path)
    ht = mt.rows()
    ht = ht.select(accession=ht.rsid, class_id=HGMD_CLASS_MAPPING[ht.info.CLASS])
    ht.annotate_globals(
        date=datetime.now().isoformat(),
        version=VERSION,
        enum_definitions=hl.dict({'hgmd': {'class_id': HGMD_CLASS_MAPPING}}),
    )
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
        chouices=['37', '38'],
    )
    args, _ = parser.parse_known_args()
    run(args.environment, args.genome_version)
