import luigi

from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.tasks.clickhouse_migration.migrate_project_to_clickhouse import (
    MigrateProjectToClickHouseTask,
)
from v03_pipeline.lib.tasks.dataproc.base_run_job_on_dataproc import (
    BaseRunJobOnDataprocTask,
)


class MigrateProjectToClickHouseOnDataprocTask(BaseRunJobOnDataprocTask):
    run_id = luigi.Parameter()
    sample_type = luigi.EnumParameter(enum=SampleType)
    project_guid = luigi.Parameter()

    @property
    def task(self) -> luigi.Task:
        return MigrateProjectToClickHouseTask
