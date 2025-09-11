import luigi

from v03_pipeline.lib.model.constants import MIGRATION_RUN_ID
from v03_pipeline.lib.tasks.clickhouse_migration.migrate_all_projects_to_clickhouse import (
    MigrateAllProjectsToClickHouseTask,
)
from v03_pipeline.lib.tasks.dataproc.base_run_job_on_dataproc import (
    BaseRunJobOnDataprocTask,
)


class MigrateAllProjectsToClickHouseOnDataprocTask(BaseRunJobOnDataprocTask):
    run_id = luigi.Parameter(default=MIGRATION_RUN_ID)

    @property
    def task(self) -> luigi.Task:
        return MigrateAllProjectsToClickHouseTask
