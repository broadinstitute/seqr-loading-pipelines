import luigi
import luigi.util

from v03_pipeline.lib.core import FeatureFlag
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.variants_migration.migrate_variant_details_parquet import (
    MigrateVariantDetailsParquetOnDataprocTask,
    MigrateVariantDetailsParquetTask,
)
from v03_pipeline.lib.tasks.variants_migration.migrate_variants_parquet import (
    MigrateVariantsParquetOnDataprocTask,
    MigrateVariantsParquetTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class LoadClickhouseVariantsTablesTask(luigi.WrapperTask):
    def requires(self) -> luigi.Task:
        return (
            [
                self.clone(MigrateVariantDetailsParquetOnDataprocTask),
                self.clone(MigrateVariantsParquetOnDataprocTask),
            ]
            if FeatureFlag.RUN_PIPELINE_ON_DATAPROC
            else [
                self.clone(MigrateVariantDetailsParquetTask),
                self.clone(MigrateVariantsParquetTask),
            ]
        )

    def run(self) -> None:
        pass
