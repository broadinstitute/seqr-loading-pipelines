import luigi

from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.delete_project_table import DeleteProjectTableTask


@luigi.util.inherits(BaseLoadingPipelineParams)
class DeleteProjectTablesTask(luigi.Task):
    project_guid = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_delete_project_table_tasks = set()

    def complete(self) -> bool:
        return len(self.dynamic_delete_project_table_tasks) >= 1 and all(
            delete_project_table_task.complete()
            for delete_project_table_task in self.dynamic_delete_project_table_tasks
        )

    def run(self):
        for sample_type in SampleType:
            self.dynamic_delete_project_table_tasks.add(
                DeleteProjectTableTask(
                    reference_genome=self.reference_genome,
                    dataset_type=self.dataset_type,
                    sample_type=sample_type,
                    project_guid=self.project_guid,
                ),
            )
        yield self.dynamic_delete_project_table_tasks
