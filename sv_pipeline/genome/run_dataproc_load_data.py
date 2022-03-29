#!/usr/bin/env python3

import argparse
from datetime import datetime
import os

from gcloud_dataproc.v02.create_cluster_without_VEP import create_cluster
from gcloud_dataproc.submit import submit

REGION = 'us-central1'

def main():
    p = argparse.ArgumentParser()
    p.add_argument('projects', nargs='+')
    p.add_argument('-c', '--cluster', default='sv-wgs-loading')
    p.add_argument('-i', '--input', required=True)
    p.add_argument('--gencode-path', default='gs://seqr-reference-data/gencode')

    args, unparsed_args = p.parse_known_args()

    cluster = args.cluster

    es_password = os.environ.get('PIPELINE_ES_PASSWORD')
    if not es_password:
        raise ValueError('ES password env variable is required')

    projects = args.projects

    os.chdir(os.path.join(os.path.dirname(__file__), '../..'))

    create_cluster(cluster=cluster, region=REGION, num_workers=2, num_preemptible_workers=len(projects))

    script_args = [
        args.input, '--use-dataproc', f'--gencode-path={args.gencode_path}', f'--es-password={es_password}',
    ] + unparsed_args
    for project in projects:
        project_script_args = script_args + [f'--project-guid={project}']
        job_id = f'sv_wgs_{project}_{datetime.now():%Y%m%d-%H%M}'
        submit('sv_pipeline/genome/load_data.py', project_script_args, cluster=cluster, job_id=job_id, region=REGION,
               wait_for_job=False, use_existing_scripts_zip=True, spark_env=f'PIPELINE_ES_PASSWORD={es_password}')

if __name__ == '__main__':
    main()
