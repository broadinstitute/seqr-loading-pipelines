#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.reference_data.constants import GCS_PREFIXES
from hail_scripts.utils.clinvar import (
    download_and_import_latest_clinvar_vcf,
    CLINVAR_GOLD_STARS_LOOKUP,
    CLINVAR_SIGNIFICANCES_LOOKUP,
)
from hail_scripts.utils.hail_utils import write_ht

CLINVAR_HT_PATH = 'clinvar/clinvar.GRCh{genome_version}.{timestamp}.ht'
PARTITIONS = 100 # per https://github.com/broadinstitute/seqr-loading-pipelines/pull/383

def run(environment: str):
    for genome_version in ['37', '38']:
        mt = download_and_import_latest_clinvar_vcf(genome_version)
        timestamp = hl.eval(mt.version)
        ht = mt.rows()
        ht.describe()
        ht = ht.transmute(
            alleleId=ht.info.select('ALLELEID'),
            clinical_significance_id=CLINVAR_SIGNIFICANCES_LOOKUP.get(hl.delimit(ht.info.CLNSIG)),
            gold_stars=CLINVAR_GOLD_STARS_LOOKUP.get(hl.delimit(ht.info.CLNREVSTAT)),
        ).select('alleleId', 'clinical_significance_id', 'gold_stars')
        ht = ht.repartition(PARTITIONS)
        destination_path = os.path.join(GCS_PREFIXES[environment], CLINVAR_HT_PATH).format(
            environment=environment,
            genome_version=genome_version,
            timestamp=timestamp,
        )
        print(f'Uploading ht from {hl.eval(ht.sourceFilePath)} to {destination_path}')
        write_ht(ht, destination_path)
        os.remove(hl.eval(ht.sourceFilePath))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--environment',
        default='dev',
        choices=['dev', 'prod']
    )
    args = parser.parse_args()
    run(args.environment)
