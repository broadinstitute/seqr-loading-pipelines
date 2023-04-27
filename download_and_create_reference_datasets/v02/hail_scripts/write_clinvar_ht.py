import tempfile

import hail as hl
from hail_scripts.reference_data.clinvar import (
    download_and_import_latest_clinvar_vcf,
    CLINVAR_HT_PATH,
    CLINVAR_GOLD_STARS_LOOKUP,
)
from hail_scripts.utils.hail_utils import write_ht


for genome_version in ["37", "38"]:
    with tempfile.NamedTemporaryFile(suffix='.vcf.gz') as tmp_file
        mt = download_and_import_latest_clinvar_vcf(genome_version, tmp_file)
        timestamp = hl.eval(mt.version)
        ht = mt.rows()
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
