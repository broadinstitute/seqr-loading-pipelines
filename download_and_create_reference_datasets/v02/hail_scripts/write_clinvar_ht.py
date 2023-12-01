import tempfile

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_data.clinvar import (
    download_and_import_latest_clinvar_vcf,
    CLINVAR_GOLD_STARS_LOOKUP,
)
from hail_scripts.utils.hail_utils import write_ht

CLINVAR_PATH = 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_{reference_genome}/clinvar.vcf.gz'
CLINVAR_HT_PATH = 'gs://seqr-reference-data/{reference_genome}/clinvar/clinvar.{reference_genome}.ht'

for reference_genome in ReferenceGenome:
    clinvar_url = CLINVAR_PATH.format(reference_genome=reference_genome.value)
    ht = download_and_import_latest_clinvar_vcf(clinvar_url, reference_genome)
    timestamp = hl.eval(ht.version)
    ht = ht.annotate(
        gold_stars=CLINVAR_GOLD_STARS_LOOKUP.get(hl.delimit(ht.info.CLNREVSTAT))
    )
    ht.describe()
    ht = ht.repartition(100)
    write_ht(
        ht,
        CLINVAR_HT_PATH.format(reference_genome=reference_genome.value).replace(".ht", ".")
        + timestamp
        + ".ht",
    )
