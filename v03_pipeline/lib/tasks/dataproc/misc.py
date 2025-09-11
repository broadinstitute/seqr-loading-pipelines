import re

import luigi

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.tasks.clickhouse_migration.migrate_all_projects_to_clickhouse import (
    MIGRATION_RUN_ID,
)

CLUSTER_NAME_PREFIX = 'pipeline-runner'


def get_cluster_name(reference_genome: ReferenceGenome, run_id: str):
    if MIGRATION_RUN_ID in run_id:
        return f'{CLUSTER_NAME_PREFIX}-{reference_genome.value.lower()}-hs-to-clckhouse'
    return f'{CLUSTER_NAME_PREFIX}-{reference_genome.value.lower()}-{run_id}'


def snake_to_kebab_arg(snake_string: str) -> str:
    return '--' + re.sub(r'\_', '-', snake_string).lower()


def to_kebab_str_args(task: luigi.Task):
    return [
        e for k, v in task.to_str_params().items() for e in (snake_to_kebab_arg(k), v)
    ]
