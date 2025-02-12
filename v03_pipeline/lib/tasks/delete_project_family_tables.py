import os

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


@luigi.util.inherits(BaseLoadingPipelineParams)
class DeleteProjectFamilyTablesTask(luigi.Task):
    sample_type = luigi.EnumParameter(enum=SampleType)
    project_guid = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_delete_family_table_tasks = set()

    def complete(self) -> bool:
        project_ht_path = project_table_path(
            self.reference_genome,
            self.dataset_type,
            self.sample_type,
            self.project_guid,
        )
        if not (
            hfs.exists(project_ht_path)
            and hfs.exists(
                os.path.join(project_ht_path, '_SUCCESS'),
            )
        ):
            return True
        return len(self.dynamic_delete_family_table_tasks) >= 1 and all(
            delete_family_table_task.complete()
            for delete_family_table_task in self.dynamic_delete_family_table_tasks
        )

    def run(self):
        project_ht = hl.read_table(
            project_table_path(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.project_guid,
            ),
        )
        family_guids = hl.eval(project_ht.globals.family_guids)
        for family_guid in family_guids:
            self.dynamic_delete_family_table_tasks.add(
                DeleteFamilyTableTask(
                    reference_genome=self.reference_genome,
                    dataset_type=self.dataset_type,
                    sample_type=self.sample_type,
                    family_guid=family_guid,
                ),
            )
        yield self.dynamic_delete_family_table_tasks
