import argparse

import hail as hl
from hail_scripts.utils.clinvar import (
    download_and_import_latest_clinvar_vcf,
    CLINVAR_GOLD_STARS_LOOKUP,
)
from hail_scripts.utils.hail_utils import write_ht

CLINVAR_HT_PATH = "gs://seqr-reference-data{seqr_reference_data_prefix}/GRCh{genome_version}/clinvar/clinvar.GRCh{genome_version}.{timestamp}.ht"

def run(seqr_reference_data_prefix):
    for genome_version in ["37", "38"]:
        mt = download_and_import_latest_clinvar_vcf(genome_version)
        timestamp = hl.eval(mt.version)
        ht = mt.rows()
        ht = ht.annotate(
            gold_stars=CLINVAR_GOLD_STARS_LOOKUP.get(hl.delimit(ht.info.CLNREVSTAT))
        )
        ht.describe()
        ht = ht.repartition(100)
        ht = ht.transmute(info=ht.info.select('ALLELEID', 'CLNSIG'))
        ht = ht.select('info', 'gold_stars')
        write_ht(
            ht,
            CLINVAR_HT_PATH.format(
                seqr_reference_data_prefix=seqr_reference_data_prefix,
                genome_version=genome_version,
                timestamp=timestamp,
            )
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--seqr-reference-data-prefix",
        default="",
        choices=["", "/dev"]
    )
    args = parser.parse_args()
    run(args.seqr_reference_data_prefix)
