import luigi

from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_delete_table import BaseDeleteTableTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class DeleteProjectTableTask(BaseDeleteTableTask):
    project_guid = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            project_table_path(
                self.reference_genome,
                self.dataset_type,
                self.project_guid,
            ),
        )
