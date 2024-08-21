import luigi

from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.delete_family_table import DeleteFamilyTableTask


@luigi.util.inherits(BaseLoadingPipelineParams)
class DeleteFamilyTablesTask(luigi.Task):
    family_guids = luigi.ListParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_delete_family_table_tasks = set()

    def complete(self) -> bool:
        return len(self.dynamic_delete_family_table_tasks) >= 1 and all(
            delete_family_table_task.complete()
            for delete_family_table_task in self.dynamic_delete_family_table_tasks
        )

    def run(self):
        for sample_type in SampleType:
            for family_guid in self.family_guids:
                self.dynamic_delete_family_table_tasks.add(
                    DeleteFamilyTableTask(
                        reference_genome=self.reference_genome,
                        dataset_type=self.dataset_type,
                        sample_type=sample_type,
                        family_guid=family_guid,
                    ),
                )
        yield self.dynamic_delete_family_table_tasks
