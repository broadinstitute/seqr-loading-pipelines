import os


FTP_PATH = "ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh{genome_version}/clinvar.vcf.gz"
GCLOUD_BUCKET_PATH = "gs://seqr-reference-data/GRCh{genome_version}/clinvar"


def run(command):
    print(command)
    os.system(command)

for genome_version in ('37', '38'):
    ftp_path = FTP_PATH.format(**locals())
    vcf_filename = "clinvar.GRCh{genome_version}.vcf.gz".format(**locals())
    gcloud_bucket_path = GCLOUD_BUCKET_PATH.format(**locals())
    run("wget {ftp_path} -O {vcf_filename}".format(**locals()))
    run("gsutil -m cp {vcf_filename} {gcloud_bucket_path}/{vcf_filename}".format(**locals()))
    run("python run_script.py convert_vcf_to_vds.py {gcloud_bucket_path}/{vcf_filename}".format(**locals()))


