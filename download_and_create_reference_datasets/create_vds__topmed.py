#!/usr/bin/env python

from hail_scripts.v01.utils.shell_utils import simple_run as run

for vcf_path in [
    "gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.vcf.gz",
    "gs://seqr-reference-data/GRCh38/TopMed/bravo-dbsnp-all.vcf.gz"
]:
    run(" ".join([
        "python gcloud_dataproc/run_script.py",
        "--cluster topmed",
        "hail_scripts/v01/convert_vcf_to_vds.py",
        "--sites-only",
        "{vcf_path}",
    ]).format(**locals()))
