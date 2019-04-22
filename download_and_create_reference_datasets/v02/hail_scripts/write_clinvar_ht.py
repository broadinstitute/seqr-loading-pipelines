#!/usr/bin/env python3

from hail_scripts.v02.utils.clinvar import download_and_import_latest_clinvar_vcf, CLINVAR_HT_PATH
from hail_scripts.v02.utils.hail_utils import write_ht

for genome_version in ["37", "38"]:
    mt = download_and_import_latest_clinvar_vcf(genome_version)

    ht = mt.rows()
    ht.describe()

    write_ht(ht, CLINVAR_HT_PATH.format(genome_version=genome_version))
