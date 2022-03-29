#!/usr/bin/env python3

import argparse
from datetime import datetime
import os
import time

from gcloud_dataproc.v02.create_cluster_without_VEP import create_cluster
from gcloud_dataproc.submit import submit

"""
Example command: 

python sv_pipeline/genome/run_dataproc_load_data.py --input=gs://seqr-datasets-gcnv/GRCh38/RDG_WGS_Broad_Internal/v1/phase2.svid.vcf.bgz \
 --strvctvre=gs://seqr-datasets-gcnv/GRCh38/RDG_WGS_Broad_Internal/v1/phase2.STRVCTRE.vcf.bgz --ignore-missing-samples \
 --es-host=10.128.0.52 R0486_cmg_gcnv R0543_aicardi_wgs R0485_cmg_beggs_wgs R0312_cmg_bonnemann_genomes R0469_cmg_engle \
 R0332_cmg_estonia_wgs R0481_cmg_gleeson_wgs R0354_cmg_hildebrandt_wgs R0550_cmg_jueppner_wgs R0316_cmg_kang_genomes \
 R0534_cmg_laing_ravencroft_wgs R0538_cmg_lerner_ellis_wgs R0359_cmg_manton_genomes R0487_cmg_myoseq_wgs R0411_cmg_pcgc_wgs \
 R0549_cmg_roscioli_wgs R0477_cmg_sankaran_genomes R0537_cmg_scott_genomes R0502_cmg_seidman_wgs R0535_cmg_sherr_wgs \
 R0530_cmg_southampton_wgs R0475_cmg_tristani_wgs R0405_cmg_vcgs_genomes R0503_cmg_walsh_wgs R0407_cmg_wendy_chung_genomes \
 R0238_inmr_wgs_pcr_free_wgs R0293_inmr_neuromuscular_disea R0279_laing_pcrfree_wgs_v5 R0470_laing_wgs R0310_manton_pcrfree_4s \
 R0428_newcastle_genomes_d309 R0311_pierce_retinal_degenerat R0546_rare_genome_project_exte R0384_rare_genomes_project_gen
"""

REGION = 'us-central1'
PROJECT_CHUNK_SIZE = 5

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
    project_chunks = [projects[ind:ind+PROJECT_CHUNK_SIZE] for ind in range(0, len(projects), PROJECT_CHUNK_SIZE)]
    for project_chunk in project_chunks:
        for project in project_chunk:
            project_script_args = script_args + [f'--project-guid={project}']
            job_id = f'sv_wgs_{project}_{datetime.now():%Y%m%d-%H%M}'
            submit('sv_pipeline/genome/load_data.py', project_script_args, cluster=cluster, job_id=job_id, region=REGION,
                   wait_for_job=False, use_existing_scripts_zip=True, spark_env=f'PIPELINE_ES_PASSWORD={es_password}')
        # Break project loading into chunks to prevent overloading ES
        print(f'Triggered loading for {len(project_chunk)} projects, waiting for next chunk')
        time.sleep(120)

    print(f'Triggered loading for all {len(projects)} projects')

if __name__ == '__main__':
    main()
