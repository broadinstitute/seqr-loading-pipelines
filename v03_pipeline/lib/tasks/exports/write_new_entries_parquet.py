import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.annotations.expression_helpers import get_expr_for_xpos
from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.family_entries import (
    compute_callset_family_entries_ht,
)
from v03_pipeline.lib.paths import (
    new_entries_parquet_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_write_parquet import BaseWriteParquetTask
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

    def requires(self) -> list[luigi.Task]:
        return {
            'annotations_table_task': (
                self.clone(UpdateVariantAnnotationsTableWithNewSamplesTask)
            ),
            'remapped_and_subsetted_callset_tasks': [
                self.clone(
                    WriteRemappedAndSubsettedCallsetTask,
                    project_i=i,
                )
                for i in range(len(self.project_guids))
            ],
            **(
                {
                    'high_af_variants_table_task': self.clone(
                        UpdatedReferenceDatasetQueryTask,
                        reference_dataset_query=ReferenceDatasetQuery.high_af_variants,
                    ),
                }
                if ReferenceDatasetQuery.high_af_variants
                in BaseReferenceDataset.for_reference_genome_dataset_type(
                    self.reference_genome,
                    self.dataset_type,
                )
                else {}
            ),
        }

    def create_table(self) -> None:
        unioned_ht = None
        for project_guid, remapped_and_subsetted_callset_task in zip(
            self.project_guids,
            self.input()['remapped_and_subsetted_callset_tasks'],
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
            ht = ht.annotate(
                family_entries=hl.enumerate(ht.family_entries).starmap(
                    lambda i, fs: hl.enumerate(fs).starmap(
                        lambda _, e: e.annotate(
                            family_guid=ht.family_guids[i],  # noqa: B023
                        ),
                    ),
                ),
            )
            annotations_ht = hl.read_table(
                self.input()['annotations_table_task'].path,
            )
            ht = ht.join(annotations_ht)

            if self.input().get('high_af_variants_table_task'):
                gnomad_high_af_ht = hl.read_table(
                    self.input()['high_af_variants_table_task'].path,
                )
                ht = ht.join(gnomad_high_af_ht, 'left')

            # the family entries ht will contain rows
            # where at least one family is defined... after explosion,
            # those rows should be removed.
            ht = ht.explode(ht.family_entries)
            ht = ht.filter(hl.is_defined(ht.family_entries))

            ht = ht.key_by()
            ht = ht.select(
                key_=ht.key_,
                project_guid=project_guid,
                family_guid=ht.family_entries.family_guid[0],
                sample_type=self.sample_type.value,
                xpos=get_expr_for_xpos(ht.locus),
                is_gnomad_gt_5_percent=hl.is_defined(ht.is_gt_5_percent),
                filters=ht.filters,
                calls=hl.Struct(
                    sampleId=ht.family_samples[ht.family_entries.family_guid[0]],
                    gt=ht.family_entries.GT.map(
                        lambda x: hl.case()
                        .when(x.is_hom_ref(), 0)
                        .when(x.is_het(), 1)
                        .when(x.is_hom_var(), 2)
                        .default(hl.missing(hl.tint32)),
                    ),
                    gq=ht.family_entries.GQ,
                    ab=ht.family_entries.AB,
                    dp=ht.family_entries.DP,
                ),
            )
            unioned_ht = unioned_ht.union(ht) if unioned_ht else ht
        return unioned_ht
