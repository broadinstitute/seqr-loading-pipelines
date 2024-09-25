import luigi
import luigi.util

from v03_pipeline.lib.misc.project_tables import get_valid_project_tables
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.update_project_table_with_deleted_families import (
    UpdateProjectTableWithDeletedFamiliesTask,
)


@luigi.util.inherits(BaseLoadingPipelineParams)
class UpdateProjectTablesWithDeletedFamiliesTask(luigi.Task):
    project_guid = luigi.Parameter()
    family_guids = luigi.ListParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_update_project_table_tasks = set()

    def complete(self) -> bool:
        return len(self.dynamic_update_project_table_tasks) >= 1 and all(
            update_project_table_task.complete()
            for update_project_table_task in self.dynamic_update_project_table_tasks
        )

    def run(self):
        project_tables = get_valid_project_tables(
            self.reference_genome,
            self.dataset_type,
            self.project_guid,
        )
        for _, sample_type in project_tables:
            self.dynamic_update_project_table_tasks.add(
                UpdateProjectTableWithDeletedFamiliesTask(
                    reference_genome=self.reference_genome,
                    dataset_type=self.dataset_type,
                    sample_type=sample_type,
                    project_guid=self.project_guid,
                    family_guids=self.family_guids,
                ),
            )
        yield self.dynamic_update_project_table_tasks
