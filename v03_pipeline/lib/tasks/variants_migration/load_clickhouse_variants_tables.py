import luigi
import luigi.util

from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.variants_migration import (
    MigrateVariantDetailsParquetTask,
    MigrateVariantsParquetTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class LoadClickhouseVariantsTablesTask(luigi.WrapperTask):
    def requires(self) -> luigi.Task:
        return [
            self.clone(MigrateVariantDetailsParquetTask),
            self.clone(MigrateVariantsParquetTask),
        ]

    def run(self) -> None:
        pass
