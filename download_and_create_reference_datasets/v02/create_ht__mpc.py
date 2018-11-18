#!/usr/bin/env python3

from kubernetes.shell_utils import simple_run as run

for genome_version, vcf_path in [
    ("37", "gs://seqr-reference-data/GRCh37/MPC/fordist_constraint_official_mpc_values.vcf.gz"),
    ("38", "gs://seqr-reference-data/GRCh38/MPC/fordist_constraint_official_mpc_values.liftover.GRCh38.vcf.gz"),
]:
    run(("python3 gcloud_dataproc/v02/run_script.py "
        "--cluster create-ht-mpc "
        "hail_scripts/v02/convert_vcf_to_hail.py "
        "--output-sites-only-ht "
        f"--genome-version {genome_version} "
        f"{vcf_path}"))
