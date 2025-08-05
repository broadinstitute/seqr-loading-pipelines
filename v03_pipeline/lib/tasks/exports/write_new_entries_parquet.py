import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.family_entries import (
    compute_callset_family_entries_ht,
    deglobalize_ids,
)
from v03_pipeline.lib.paths import (
    new_entries_parquet_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_write_parquet import BaseWriteParquetTask
from v03_pipeline.lib.tasks.exports.fields import get_entries_export_fields
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_query import (
    UpdatedReferenceDatasetQueryTask,
)
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
)
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)

ANNOTATIONS_TABLE_TASK = 'annotations_table_task'
REMAPPED_AND_SUBSETTED_CALLSET_TASKS = 'remapped_and_subsetted_callset_tasks'


@luigi.util.inherits(BaseLoadingRunParams)
class WriteNewEntriesParquetTask(BaseWriteParquetTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            new_entries_parquet_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self) -> dict[str, luigi.Task]:
        return {
            ANNOTATIONS_TABLE_TASK: self.clone(
                UpdateVariantAnnotationsTableWithNewSamplesTask,
            ),
            REMAPPED_AND_SUBSETTED_CALLSET_TASKS: [
                self.clone(
                    WriteRemappedAndSubsettedCallsetTask,
                    project_i=i,
                )
                for i in range(len(self.project_guids))
            ],
        }

    def create_table(self) -> None:
        unioned_ht = None
        for project_guid, remapped_and_subsetted_callset_task in zip(
            self.project_guids,
            self.input()[REMAPPED_AND_SUBSETTED_CALLSET_TASKS],
            strict=True,
        ):
            mt = hl.read_matrix_table(remapped_and_subsetted_callset_task.path)
            ht = compute_callset_family_entries_ht(
                self.dataset_type,
                mt,
                get_fields(
                    mt,
                    self.dataset_type.genotype_entry_annotation_fns,
                    **self.param_kwargs,
                ),
            )
            ht = deglobalize_ids(ht)
            annotations_ht = hl.read_table(
                variant_annotations_table_path(
                    self.reference_genome,
                    self.dataset_type,
                ),
            )
            ht = ht.join(annotations_ht)

            # the family entries ht will contain rows
            # where at least one family is defined... after explosion,
            # rows where a family is not defined should be removed.
            ht = ht.explode(ht.family_entries)
            ht = ht.filter(hl.is_defined(ht.family_entries))
            ht = ht.key_by()
            ht = ht.select_globals()
            ht = ht.select(
                **get_entries_export_fields(
                    ht,
                    self.dataset_type,
                    self.sample_type,
                    project_guid,
                ),
            )
            unioned_ht = unioned_ht.union(ht) if unioned_ht else ht
        return unioned_ht
