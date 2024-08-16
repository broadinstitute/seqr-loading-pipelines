import hail as hl
import hailtop.fs as hfs
import luigi
import luigi.util

from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.delete_family_table import DeleteFamilyTableTask
from v03_pipeline.lib.tasks.files import HailTableTask


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
        project_tables = set()
        for sample_type in SampleType:
            project_ht_path = project_table_path(
                self.reference_genome,
                self.dataset_type,
                sample_type,
                self.project_guid,
            )
            if hfs.exists(project_ht_path):
                project_table_task: luigi.Target = yield HailTableTask(project_ht_path)
                project_ht = hl.read_table(project_table_task.path)
                project_tables.add((project_ht, sample_type))

        if len(project_tables) == 0:
            msg = f'No project tables found for {self.project_guid}'
            raise RuntimeError(msg)

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
