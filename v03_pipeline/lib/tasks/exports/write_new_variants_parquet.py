import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.paths import (
    new_variants_parquet_path,
    new_variants_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_write_parquet import BaseWriteParquetTask
from v03_pipeline.lib.tasks.exports.fields import get_variants_export_fields
from v03_pipeline.lib.tasks.exports.misc import (
    camelcase_array_structexpression_fields,
    subset_filterable_transcripts_fields,
    unmap_formatting_annotation_enums,
    unmap_reference_dataset_annotation_enums,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.update_new_variants_with_caids import (
    UpdateNewVariantsWithCAIDsTask,
)
from v03_pipeline.lib.tasks.write_new_variants_table import WriteNewVariantsTableTask


@luigi.util.inherits(BaseLoadingRunParams)
class WriteNewVariantsParquetTask(BaseWriteParquetTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            new_variants_parquet_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return [
            self.clone(UpdateNewVariantsWithCAIDsTask)
            if self.dataset_type.should_send_to_allele_registry
            else self.clone(WriteNewVariantsTableTask),
        ]

    def create_table(self) -> None:
        ht = hl.read_table(
            new_variants_table_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )
        ht = unmap_formatting_annotation_enums(
            ht,
            self.reference_genome,
            self.dataset_type,
        )
        ht = unmap_reference_dataset_annotation_enums(
            ht,
            self.reference_genome,
            self.dataset_type,
        )
        ht = camelcase_array_structexpression_fields(
            ht,
            self.reference_genome,
            self.dataset_type,
        )
        ht = subset_filterable_transcripts_fields(
            ht,
            self.reference_genome,
            self.dataset_type,
        )
        ht = ht.key_by()
        return ht.select(
            **get_variants_export_fields(ht, self.reference_genome, self.dataset_type),
        )
