import os


DOWNLOAD_PATH = "ftp://ftp.broadinstitute.org/pub/ExAC_release/release0.3.1/functional_gene_constraint/fordist_cleaned_exac_r03_march16_z_pli_rec_null_data.txt"
GCLOUD_BUCKET_PATH = "gs://seqr-reference-data/gene_constraint"


def run(command):
    print(command)
    os.system(command)


filename = os.path.basename(DOWNLOAD_PATH)

run("wget -O {filename} {DOWNLOAD_PATH}".format(**locals()))

run("""/bin/bash -c "cat {filename} | sed 's/\(ENST[0-9]*\)\.[0-9]/\\1/' > {filename}.temp" """.format(**locals()))
run("mv {filename}.temp {filename}".format(**locals()))
run("gsutil -m cp {filename} {GCLOUD_BUCKET_PATH}/{filename}".format(**locals()))

os.chdir(os.path.join(os.path.dirname(__file__), ".."))
run(" ".join([
    "python gcloud_dataproc/run_script.py",
    "--cluster gene-constraint",
    "hail_scripts/convert_tsv_to_key_table.py",
    "--key-by 'transcript'",
    "{GCLOUD_BUCKET_PATH}/{filename}",
]).format(**locals()))
