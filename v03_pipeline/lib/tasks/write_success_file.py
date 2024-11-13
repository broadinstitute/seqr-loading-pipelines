import luigi
import luigi.util

from v03_pipeline.lib.paths import pipeline_run_success_file_path
from v03_pipeline.lib.tasks import WriteProjectFamilyTablesTask
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
)
from v03_pipeline.lib.tasks.write_metadata_for_run import WriteMetadataForRunTask


@luigi.util.inherits(BaseLoadingRunParams)
class WriteSuccessFileTask(luigi.Task):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            pipeline_run_success_file_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self):
        requirements = [
            self.clone(WriteMetadataForRunTask),
            self.clone(UpdateVariantAnnotationsTableWithNewSamplesTask),
        ]
        return [
            *requirements,
            *[
                self.clone(
                    WriteProjectFamilyTablesTask,
                    project_i=i,
                )
                for i in range(len(self.project_guids))
            ],
        ]

    def run(self):
        with self.output().open('w') as f:
            f.write('')
