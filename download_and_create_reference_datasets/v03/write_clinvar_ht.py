#!/usr/bin/env python3
import argparse
import os
import tempfile

import hail as hl

from hail_scripts.reference_data.clinvar import (
    CLINVAR_ASSERTIONS,
    CLINVAR_ASSERTIONS_LOOKUP,
    CLINVAR_DEFAULT_PATHOGENICITY,
    CLINVAR_GOLD_STARS_LOOKUP,
    CLINVAR_PATHOGENICITIES,
    CLINVAR_PATHOGENICITIES_LOOKUP,
    download_and_import_latest_clinvar_vcf,
    parsed_clnsig,
    parsed_clnsigconf,
)
from hail_scripts.reference_data.config import GCS_PREFIXES, AccessControl
from hail_scripts.utils.hail_utils import write_ht

CLINVAR_HT_PATH = 'clinvar/clinvar.GRCh{genome_version}.{timestamp}.ht'
PARTITIONS = (
    100  # per https://github.com/broadinstitute/seqr-loading-pipelines/pull/383
)


def run(environment: str):
    for genome_version in ['37', '38']:
        with tempfile.NamedTemporaryFile(suffix='.vcf.gz') as tmp_file:
            mt = download_and_import_latest_clinvar_vcf(genome_version, tmp_file)
            timestamp = hl.eval(mt.version)
            ht = mt.rows()
            clnsigs = parsed_clnsig(ht)
            ht = (
                ht.select(
                    alleleId=ht.info.ALLELEID,
                    pathogenicity_id=CLINVAR_PATHOGENICITIES_LOOKUP.get(
                        clnsigs[0],
                        CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_DEFAULT_PATHOGENICITY],
                    ),
                    assertion_ids=hl.if_else(
                        CLINVAR_PATHOGENICITIES_LOOKUP.contains(clnsigs[0]),
                        clnsigs[1:],
                        clnsigs,
                    ).map(lambda x: CLINVAR_ASSERTIONS_LOOKUP[x]),
                    conflictingPathogenicities=parsed_clnsigconf(ht),
                    goldStars=CLINVAR_GOLD_STARS_LOOKUP.get(
                        hl.delimit(ht.info.CLNREVSTAT),
                    ),
                )
                .annotate_globals(
                    enum_definitions=hl.dict(
                        {
                            'assertions': CLINVAR_ASSERTIONS,
                            'pathogenicities': CLINVAR_PATHOGENICITIES,
                        },
                    ),
                )
                .repartition(
                    PARTITIONS,
                )
            )
            destination_path = os.path.join(
                GCS_PREFIXES[(environment, AccessControl.PUBLIC)],
                CLINVAR_HT_PATH,
            ).format(
                environment=environment,
                genome_version=genome_version,
                timestamp=timestamp,
            )
            print(
                f'Uploading ht from {hl.eval(ht.sourceFilePath)} to {destination_path}',
            )
            write_ht(ht, destination_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', default='dev', choices=['dev', 'prod'])
    args, _ = parser.parse_known_args()
    run(args.environment)
