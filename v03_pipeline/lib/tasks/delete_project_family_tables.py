import hail as hl
import luigi

from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.task import BaseTask
from v03_pipeline.lib.tasks.delete_family_table import DeleteFamilyTableTask
from v03_pipeline.lib.tasks.files import HailTableTask


class DeleteProjectFamilyTablesTask(BaseTask):
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
        project_table_task: luigi.Target = yield HailTableTask(
            project_table_path(
                self.reference_genome,
                self.dataset_type,
                self.project_guid,
            ),
        )
        project_ht = hl.read_table(project_table_task.path)
        family_guids = hl.eval(project_ht.globals.family_guids)
        for family_guid in family_guids:
            self.dynamic_delete_family_table_tasks.add(
                DeleteFamilyTableTask(
                    reference_genome=self.reference_genome,
                    dataset_type=self.dataset_type,
                    family_guid=family_guid,
                ),
            )
        yield self.dynamic_delete_family_table_tasks
