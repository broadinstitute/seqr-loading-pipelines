import hailtop.fs as hfs
import luigi
import luigi.util

from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.model.feature_flag import FeatureFlag
from v03_pipeline.lib.paths import (
    project_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.clickhouse_migration.migrate_project_to_clickhouse import (
    MigrateProjectToClickHouseTask,
)
from v03_pipeline.lib.tasks.clickhouse_migration.migrate_project_to_clickhouse_on_dataproc import (
    MigrateProjectToClickHouseOnDataprocTask,
)
from v03_pipeline.lib.tasks.dataproc.create_dataproc_cluster import (
    CreateDataprocClusterTask,
)

MIGRATION_RUN_ID = 'hail_search_to_clickhouse_migration'


@luigi.util.inherits(BaseLoadingPipelineParams)
class MigrateAllProjectsToClickHouseTask(luigi.WrapperTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_parquet_tasks = set()

    def requires(self) -> [luigi.Task]:
        return (
            [self.clone(CreateDataprocClusterTask)]
            if FeatureFlag.RUN_PIPELINE_ON_DATAPROC
            else []
        )

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
                        MigrateProjectToClickHouseOnDataprocTask,
                        run_id=f'{MIGRATION_RUN_ID}_{sample_type.value}_{project_guid}',
                        sample_type=sample_type,
                        project_guid=project_guid,
                    )
                    if FeatureFlag.RUN_PIPELINE_ON_DATAPROC
                    else self.clone(
                        MigrateProjectToClickHouseTask,
                        run_id=f'{MIGRATION_RUN_ID}_{sample_type.value}_{project_guid}',
                        sample_type=sample_type,
                        project_guid=project_guid,
                    ),
                )
        yield self.dynamic_parquet_tasks
