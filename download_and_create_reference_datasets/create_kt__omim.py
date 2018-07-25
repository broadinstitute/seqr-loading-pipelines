import os
import sys
from hail_scripts.utils.shell_utils import simple_run as run

if len(sys.argv) < 2:
    sys.exit("Must provide OMIM download key as command line arg (https://www.omim.org/downloads/)")

omim_download_key = sys.argv[1]

DOWNLOAD_PATH = "https://data.omim.org/downloads/%(omim_download_key)s/genemap2.txt" % locals()
GCLOUD_BUCKET_PATH = "gs://seqr-reference-data/omim"


filename = os.path.basename(DOWNLOAD_PATH)

run("wget -O {filename} {DOWNLOAD_PATH}".format(**locals()))

run("""/bin/bash -c "cat <(grep '^# Chromosome.*Genomic' {filename}) <(grep -v '^#' {filename}) > {filename}.temp" """.format(**locals()))
run("mv {filename}.temp {filename}".format(**locals()))

run("gsutil -m cp {filename} {GCLOUD_BUCKET_PATH}/{filename}".format(**locals()))

run(" ".join([
    "python gcloud_dataproc/run_script.py",
    "--cluster omim",
    "hail_scripts/convert_tsv_to_key_table.py",
    "--key-by 'Ensembl Gene ID'",
    "{GCLOUD_BUCKET_PATH}/{filename}"
]).format(**locals()))
