import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.misc.project_tables import get_valid_project_tables
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.delete_family_table import DeleteFamilyTableTask


@luigi.util.inherits(BaseLoadingPipelineParams)
class DeleteProjectFamilyTablesTask(luigi.Task):
    project_guid = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_delete_family_table_tasks = set()

    def complete(self) -> bool:
        return len(self.dynamic_delete_family_table_tasks) >= 1 and all(
            delete_family_table_task.complete()
            for delete_family_table_task in self.dynamic_delete_family_table_tasks
        )

    def run(self):
        project_tables = get_valid_project_tables(
            self.reference_genome,
            self.dataset_type,
            self.project_guid,
        )
        for project_ht, sample_type in project_tables:
            family_guids = hl.eval(project_ht.globals.family_guids)
            for family_guid in family_guids:
                self.dynamic_delete_family_table_tasks.add(
                    DeleteFamilyTableTask(
                        reference_genome=self.reference_genome,
                        dataset_type=self.dataset_type,
                        sample_type=sample_type,
                        family_guid=family_guid,
                    ),
                )
        yield self.dynamic_delete_family_table_tasks
