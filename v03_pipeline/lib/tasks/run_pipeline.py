import luigi
import luigi.util

from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
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
