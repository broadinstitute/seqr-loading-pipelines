import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.paths import (
    new_variant_details_parquet_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_write_parquet import BaseWriteParquetTask
from v03_pipeline.lib.tasks.exports.fields import get_variant_details_export_fields
from v03_pipeline.lib.tasks.exports.misc import (
    camelcase_array_structexpression_fields,
    unmap_formatting_annotation_enums,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.variants_migration.update_variant_annotations_table_with_dropped_reference_datasets import (
    UpdateVariantAnnotationsTableWithDroppedReferenceDatasetsTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class MigrateVariantsParquetTask(BaseWriteParquetTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            new_variant_details_parquet_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self) -> luigi.Task:
        return self.clone(UpdateVariantAnnotationsTableWithDroppedReferenceDatasetsTask)

    def create_table(self) -> None:
        ht = hl.read_table(
            variant_annotations_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )
        ht = unmap_formatting_annotation_enums(
            ht,
            self.reference_genome,
            self.dataset_type,
        )
        ht = camelcase_array_structexpression_fields(
            ht,
            self.reference_genome,
            self.dataset_type,
        )
        ht = ht.key_by()
        return ht.select(
            **get_variant_details_export_fields(
                ht,
                self.reference_genome,
                self.dataset_type,
            ),
        )
