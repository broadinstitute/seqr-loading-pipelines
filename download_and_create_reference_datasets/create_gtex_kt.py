import os


def run(command):
    print(command)
    os.system(command)


for vcf_path in [
    "gs://seqr-reference-data/GRCh37/MPC/fordist_constraint_official_mpc_values.vcf.gz",
    "gs://seqr-reference-data/GRCh38/MPC/fordist_constraint_official_mpc_values.liftover.GRCh38.vcf.gz"
]:
    os.chdir(os.path.join(os.path.dirname(__file__), ".."))
    run("python gcloud_dataproc/run_script.py hail_scripts/convert_vcf_to_vds.py {vcf_path}".format(**locals()))
