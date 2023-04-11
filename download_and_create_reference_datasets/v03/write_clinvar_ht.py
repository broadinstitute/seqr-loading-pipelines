#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.reference_data.constants import GCS_PREFIXES
from hail_scripts.utils.clinvar import (
    download_and_import_latest_clinvar_vcf,
    CLINVAR_CLINICAL_SIGNIFICANCES_LOOKUP,
    CLINVAR_CLINICAL_SIGNIFICANCE_MODIFIERS_LOOKUP,
    CLINVAR_GOLD_STARS_LOOKUP,
)
from hail_scripts.utils.hail_utils import write_ht

CLINVAR_HT_PATH = 'clinvar/clinvar.GRCh{genome_version}.{timestamp}.ht'
PARTITIONS = 100 # per https://github.com/broadinstitute/seqr-loading-pipelines/pull/383

def parsed_clnsig(ht: hl.Table):
    return ht.info.CLNSIG.flatmap(lambda x: x.split(r'\|')).map(lambda x: x.replace(r'$_', ''))

def run(environment: str):
    for genome_version in ['37', '38']:
        mt = download_and_import_latest_clinvar_vcf(genome_version)
        timestamp = hl.eval(mt.version)
        ht = mt.rows()
        ht.describe()
        ht = ht.annotate(
            alleleId=ht.info.select('ALLELEID'),
            clinicalSignificance_id=CLINVAR_CLINICAL_SIGNIFICANCES_LOOKUP.get(parsed_clnsig(ht)[0]),
            clinicalSignifanceModifier_ids=parsed_clnsig(ht).map(lambda x: CLINVAR_CLINICAL_SIGNIFICANCE_MODIFIERS_LOOKUP.get(x)).filter(hl.is_defined),
            goldStars=CLINVAR_GOLD_STARS_LOOKUP.get(hl.delimit(ht.info.CLNREVSTAT)),
        ).select('alleleId', 'clinicalSignificance_id', 'goldStars')
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
