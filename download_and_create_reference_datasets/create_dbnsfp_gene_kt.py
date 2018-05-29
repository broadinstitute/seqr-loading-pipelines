import os


def run(command):
    print(command)
    os.system(command)


os.chdir(os.path.join(os.path.dirname(__file__), ".."))

for dbnsfp_gene_table_path in [
    "gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9_gene",
    "gs://seqr-reference-data/GRCh38/dbNSFP/v3.5/dbNSFP3.5_gene"
]:
    run(" ".join([
        "python gcloud_dataproc/run_script.py",
        "--cluster dbnsfp",
        "hail_scripts/convert_tsv_to_key_table.py",
        "{dbnsfp_gene_table_path}"
    ]).format(**locals()))
