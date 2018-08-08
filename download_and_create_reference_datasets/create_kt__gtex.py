from hail_scripts.v01.utils.shell_utils import simple_run as run


gtex_table_path = "gs://seqr-reference-data/gtex/GTEx_Analysis_2016-01-15_v7_RNASeQCv1.1.8_gene_tpm.gct.gz"

#os.chdir(os.path.join(os.path.dirname(__file__), ".."))
#run("python gcloud_dataproc/run_script.py hail_scripts/v01/convert_tsv_to_key_table.py {gtex_table_path}".format(**locals()))
