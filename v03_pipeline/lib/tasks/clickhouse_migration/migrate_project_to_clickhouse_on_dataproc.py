import luigi

from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.clickhouse_migration.migrate_project_to_clickhouse import (
    MigrateProjectToClickHouseTask,
)
from v03_pipeline.lib.tasks.dataproc.base_run_job_on_dataproc import (
    BaseRunJobOnDataprocTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class MigrateProjectToClickHouseOnDataprocTask(BaseRunJobOnDataprocTask):
    @property
    def task(self) -> luigi.Task:
        return MigrateProjectToClickHouseTask
