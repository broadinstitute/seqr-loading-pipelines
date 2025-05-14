import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.misc.family_entries import (
    deglobalize_ids,
)
from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.paths import (
    new_entries_parquet_path,
    project_table_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.base.base_write_parquet import BaseWriteParquetTask
from v03_pipeline.lib.tasks.exports.fields import get_entries_export_fields
from v03_pipeline.lib.tasks.exports.write_new_entries_parquet import (
    ANNOTATIONS_TABLE_TASK,
    HIGH_AF_VARIANTS_TABLE_TASK,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, HailTableTask
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_query import (
    UpdatedReferenceDatasetQueryTask,
)

PROJECT_TABLE_TASK = 'project_table_task'


@luigi.util.inherits(BaseLoadingPipelineParams)
class MigrateProjectEntriesToClickHouseTask(BaseWriteParquetTask):
    run_id = luigi.Parameter()
    sample_type = luigi.EnumParameter(enum=SampleType)
    project_guid = luigi.Parameter()

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
            ANNOTATIONS_TABLE_TASK: HailTableTask(
                variant_annotations_table_path(
                    self.reference_genome,
                    self.dataset_type,
                ),
            ),
            PROJECT_TABLE_TASK: HailTableTask(
                project_table_path(
                    self.reference_genome,
                    self.dataset_type,
                    self.sample_type,
                    self.project_guid,
                ),
            ),
            **(
                {
                    HIGH_AF_VARIANTS_TABLE_TASK: self.clone(
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
        ht = hl.read_table(
            self.input()[PROJECT_TABLE_TASK].path,
        )
        ht = deglobalize_ids(ht)
        annotations_ht = hl.read_table(
            self.input()[ANNOTATIONS_TABLE_TASK].path,
        )
        ht = ht.join(annotations_ht)
        if self.input().get(HIGH_AF_VARIANTS_TABLE_TASK):
            gnomad_high_af_ht = hl.read_table(
                self.input()[HIGH_AF_VARIANTS_TABLE_TASK].path,
            )
            ht = ht.join(gnomad_high_af_ht, 'left')

        ht = ht.explode(ht.family_entries)
        ht = ht.filter(hl.is_defined(ht.family_entries))
        ht = ht.key_by()
        ht = ht.select_globals()
        return ht.select(
            **get_entries_export_fields(
                ht,
                self.dataset_type,
                self.sample_type,
                self.project_guid,
            ),
        )
