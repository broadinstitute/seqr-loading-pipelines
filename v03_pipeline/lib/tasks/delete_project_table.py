import luigi

from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_delete_table import BaseDeleteTableTask
from v03_pipeline.lib.tasks.delete_project_family_tables import (
    DeleteProjectFamilyTablesTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class DeleteProjectTableTask(BaseDeleteTableTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    project_guid = luigi.Parameter()

    def requires(self) -> luigi.Task:
        return DeleteProjectFamilyTablesTask(
            self.reference_genome,
            self.dataset_type,
            self.sample_type,
            self.project_guid,
        )

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            project_table_path(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.project_guid,
            ),
        )
