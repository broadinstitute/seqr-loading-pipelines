import json

import hailtop.fs as hfs
import luigi
import luigi.util

from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.paths import (
    metadata_for_run_path,
    pipeline_run_success_file_path,
    project_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.clickhouse_migration.constants import (
    ClickHouseMigrationType,
)
from v03_pipeline.lib.tasks.clickhouse_migration.migrate_project_entries_to_clickhouse import (
    MigrateProjectEntriesToClickHouseTask,
)


@luigi.util.inherits(BaseLoadingPipelineParams)
class MigrateAllProjectEntriesToClickHouseTask(luigi.WrapperTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_parquet_tasks = set()

    def complete(self):
        return len(self.dynamic_parquet_tasks) >= 1 and all(
            dynamic_parquet_tasks.complete()
            for dynamic_parquet_tasks in self.dynamic_parquet_tasks
        )

    def run(self):
        for sample_type in SampleType:
            for p in hfs.ls(
                project_table_path(
                    self.reference_genome,
                    self.dataset_type,
                    sample_type,
                    '*',
                ),
            ):
                project_guid = p.path.split('/')[-1].replace('.ht', '')
                self.dynamic_parquet_tasks.add(
                    self.clone(
                        MigrateProjectEntriesToClickHouseTask,
                        run_id=f'{ClickHouseMigrationType.PROJECT_ENTRIES.run_id}_{sample_type.value}_{project_guid}',
                        sample_type=sample_type,
                        project_guid=project_guid,
                    ),
                )
        yield self.dynamic_parquet_tasks
        for task in self.dynamic_parquet_tasks:
            metadata_json = {
                'migration_type': ClickHouseMigrationType.PROJECT_ENTRIES.value,
                'callsets': [],
                'run_id': f'{ClickHouseMigrationType.PROJECT_ENTRIES.run_id}_{task.sample_type.value}_{task.project_guid}',
                'sample_type': task.sample_type.value,
                'project_guids': [task.project_guid],
                'family_samples': {},
                'failed_family_samples': {
                    'missing_samples': {},
                    'relatedness_check': {},
                    'sex_check': {},
                    'ploidy_check': {},
                },
                'relatedness_check_file_path': '',
                'sample_qc': {},
            }
            path = metadata_for_run_path(
                self.reference_genome,
                self.dataset_type,
                f'{ClickHouseMigrationType.PROJECT_ENTRIES.run_id}_{task.sample_type.value}_{task.project_guid}',
            )
            with hfs.open(path, mode='w') as f:
                json.dump(metadata_json, f)
            path = pipeline_run_success_file_path(
                self.reference_genome,
                self.dataset_type,
                f'{ClickHouseMigrationType.PROJECT_ENTRIES.run_id}_{task.sample_type.value}_{task.project_guid}',
            )
            with hfs.open(path, mode='w') as f:
                f.write('')
