#!/usr/bin/env python3
import argparse

import hail as hl

from hail_scripts.reference_data.constants import V03_GCS_PREFIX
from hail_scripts.utils.clinvar import (
    download_and_import_latest_clinvar_vcf,
    CLINVAR_GOLD_STARS_LOOKUP,
)
from hail_scripts.utils.hail_utils import write_ht

CLINVAR_HT_PATH = os.path.join(V03_GCS_PREFIX, 'clinvar/clinvar.GRCh{genome_version}.{timestamp}.ht')

def run(environment: str):
    for genome_version in ['37', '38']:
        mt = download_and_import_latest_clinvar_vcf(genome_version)
        timestamp = hl.eval(mt.version)
        ht = mt.rows()
        ht = ht.annotate(
            gold_stars=CLINVAR_GOLD_STARS_LOOKUP.get(hl.delimit(ht.info.CLNREVSTAT))
        )
        ht.describe()
        ht = ht.transmute(info=ht.info.select('ALLELEID', 'CLNSIG')).select('info', 'gold_stars')
        ht = ht.repartition(100)
        destination_path = CLINVAR_HT_PATH.format(
            environment=environment,
            genome_version=genome_version,
            timestamp=timestamp,
        )
        print(f'Uploading ht from {hl.eval(ht.sourceFilePath)} to {destination_path}')
        write_ht(ht, destination_path)
        os.remove(ht.sourceFilePath)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--environment',
        default='dev',
        choices=['dev', 'prod']
    )
    args = parser.parse_args()
    run(args.environment)
