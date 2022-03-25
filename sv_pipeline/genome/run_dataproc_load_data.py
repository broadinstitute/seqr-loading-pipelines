#!/usr/bin/env python3

import argparse
import os

from kubernetes.shell_utils import simple_run as run

def main():
    p = argparse.ArgumentParser()
    p.add_argument('-c', '--cluster', default='sv-wgs-loading')
    p.add_argument('-i', '--input', required=True)
    p.add_argument('projects', nargs='+')

    args, unparsed_args = p.parse_known_args()

    cluster = args.cluster
    script_args = ' '.join(unparsed_args)

    es_password = os.environ.get('PIPELINE_ES_PASSWORD')
    if not es_password:
        raise ValueError('ES password env variable is required')

    projects = args.projects

    os.chdir(os.path.join(os.path.dirname(__file__), '../..'))

    run(f'./gcloud_dataproc/v02/create_cluster_without_VEP.py {cluster} 2 {len(projects)}')

    for project in projects:
        command = f'sv_pipeline/genome/load_data.py {args.input} --use-dataproc --project-guid={project} {script_args}'
        run(f'time ./gcloud_dataproc/submit.py --cluster={cluster} --spark-env=PIPELINE_ES_PASSWORD={es_password} {command}')

if __name__ == '__main__':
    main()