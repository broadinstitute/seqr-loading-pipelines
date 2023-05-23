import tempfile

import hail as hl
from hail_scripts.reference_data.clinvar import (
    download_and_import_latest_clinvar_vcf,
    CLINVAR_FTP_PATH,
    CLINVAR_HT_PATH,
    CLINVAR_GOLD_STARS_LOOKUP,
)
from hail_scripts.utils.hail_utils import write_ht


for genome_version in ["37", "38"]:
    clinvar_url = CLINVAR_FTP_PATH.format(genome_version=genome_version)
    ht = download_and_import_latest_clinvar_vcf(clinvar_url, genome_version)
    timestamp = hl.eval(ht.version)
    ht = ht.annotate(
        gold_stars=CLINVAR_GOLD_STARS_LOOKUP.get(hl.delimit(ht.info.CLNREVSTAT))
    )
    ht.describe()
    ht = ht.repartition(100)
    write_ht(
        ht,
        CLINVAR_HT_PATH.format(genome_version=genome_version).replace(".ht", ".")
        + timestamp
        + ".ht",
    )
