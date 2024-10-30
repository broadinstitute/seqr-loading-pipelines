import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.paths import remapped_and_subsetted_callset_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.update_project_table import UpdateProjectTableTask
from v03_pipeline.lib.tasks.write_family_table import WriteFamilyTableTask
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class WriteProjectFamilyTablesTask(luigi.Task):
    project_i = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_write_family_table_tasks = set()

    def complete(self) -> bool:
        return len(self.dynamic_write_family_table_tasks) >= 1 and all(
            write_family_table_task.complete()
            for write_family_table_task in self.dynamic_write_family_table_tasks
        )

    def requires(self) -> list[luigi.Task]:
        return [
            self.clone(
                WriteRemappedAndSubsettedCallsetTask,
            ),
            self.clone(
                UpdateProjectTableTask,
            ),
        ]

    def run(self):
        ht = hl.read_matrix_table(
            remapped_and_subsetted_callset_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
                self.project_guid,
            ),
        )
        for family_guid in set(hl.eval(ht.globals.family_samples).keys()):
            self.dynamic_write_family_table_tasks.add(
                self.clone(WriteFamilyTableTask, family_guid=family_guid),
            )
        yield self.dynamic_write_family_table_tasks
