import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.misc.callsets import get_callset_ht
from v03_pipeline.lib.paths import (
    new_variants_parquet_path,
    new_variants_table_path,
    variant_annotations_table_path,
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
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
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

    def requires(self) -> luigi.Task:
        if self.dataset_type.export_all_callset_variants:
            return self.clone(UpdateVariantAnnotationsTableWithNewSamplesTask)
        if self.dataset_type.should_send_to_allele_registry:
            return self.clone(UpdateNewVariantsWithCAIDsTask)
        return self.clone(WriteNewVariantsTableTask)

    def create_table(self) -> None:
        if self.dataset_type.export_all_callset_variants:
            ht = hl.read_table(
                variant_annotations_table_path(
                    self.reference_genome,
                    self.dataset_type,
                ),
            )
            callset_ht = get_callset_ht(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
                self.project_guids,
            )
            ht = ht.semi_join(callset_ht)
        else:
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
