from hail_scripts.utils.shell_utils import simple_run as run

run(" ".join([
    "python gcloud_dataproc/run_script.py",
    "--cluster dbnsfp",
    "download_and_create_reference_datasets/hail_scripts/write_dbnsfp_vds.py",
]))
