#!/usr/bin/env python3

from kubernetes.shell_utils import simple_run as run

for genome_version, vcf_path in [
    ("37", "gs://seqr-reference-data/GRCh37/primate_ai/PrimateAI_scores_v0.2.vcf.gz"),
    ("38", "gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.vcf.gz"),
]:
    run(("python3 gcloud_dataproc/v02/run_script.py "
        "--cluster create-ht-primate-ai "
        "hail_scripts/v02/convert_vcf_to_hail.py "
        "--output-sites-only-ht "
        f"--genome-version {genome_version} "
        f"{vcf_path}"))
