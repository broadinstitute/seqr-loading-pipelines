import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.tasks.base.base_hail_table import BaseHailTableTask
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.update_project_table import UpdateProjectTableTask
from v03_pipeline.lib.tasks.write_family_table import WriteFamilyTableTask


@luigi.util.inherits(BaseLoadingRunParams)
class WriteProjectFamilyTablesTask(BaseHailTableTask):
    project_guid = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_write_family_table_tasks = set()

    def complete(self) -> bool:
        return (
            not self.force
            and len(self.dynamic_write_family_table_tasks) >= 1
            and all(
                write_family_table_task.complete()
                for write_family_table_task in self.dynamic_write_family_table_tasks
            )
        )

    def run(self):
        # https://luigi.readthedocs.io/en/stable/tasks.html#dynamic-dependencies
        update_project_table_task: luigi.Target = yield self.clone(
            UpdateProjectTableTask,
            force=False,
        )
        project_ht = hl.read_table(update_project_table_task.path)
        family_guids = hl.eval(project_ht.globals.family_guids)
        for family_guid in family_guids:
            self.dynamic_write_family_table_tasks.add(
                self.clone(WriteFamilyTableTask, family_guid=family_guid),
            )
        yield self.dynamic_write_family_table_tasks
