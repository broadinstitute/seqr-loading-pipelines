from v03_pipeline.lib.model import ReferenceGenome

CLUSTER_NAME_PREFIX = 'pipeline-runner'


def get_cluster_name(reference_genome: ReferenceGenome, run_id: str):
    return f'{CLUSTER_NAME_PREFIX}-{reference_genome.value.lower()}-{run_id}'
