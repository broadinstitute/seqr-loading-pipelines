import luigi
import luigi.util

from v03_pipeline.lib.model.feature_flag import FeatureFlag
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDataset,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.exports.write_new_entries_parquet import (
    WriteNewEntriesParquetTask,
)
from v03_pipeline.lib.tasks.exports.write_new_transcripts_parquet import (
    WriteNewTranscriptsParquetTask,
)
from v03_pipeline.lib.tasks.exports.write_new_variants_parquet import (
    WriteNewVariantsParquetTask,
)
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
)
from v03_pipeline.lib.tasks.write_metadata_for_run import WriteMetadataForRunTask
from v03_pipeline.lib.tasks.write_project_family_tables import (
    WriteProjectFamilyTablesTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class RunPipelineTask(luigi.WrapperTask):
    def requires(self):
        return [
            self.clone(WriteMetadataForRunTask),
            self.clone(UpdateVariantAnnotationsTableWithNewSamplesTask),
            *[
                self.clone(
                    WriteProjectFamilyTablesTask,
                    project_i=i,
                )
                for i in range(len(self.project_guids))
            ],
            *(
                [self.clone(WriteNewTranscriptsParquetTask)]
                if FeatureFlag.EXPORT_TO_PARQUET
                and self.dataset_type.should_write_new_transcripts
                else []
            ),
            *(
                [
                    self.clone(WriteNewEntriesParquetTask),
                    self.clone(WriteNewVariantsParquetTask),
                ]
                if FeatureFlag.EXPORT_TO_PARQUET
                else []
            ),
        ]
