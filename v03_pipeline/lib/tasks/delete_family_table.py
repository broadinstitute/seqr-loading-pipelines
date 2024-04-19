import luigi

from v03_pipeline.lib.paths import family_table_path
from v03_pipeline.lib.tasks.base.base_delete_task import BaseDeleteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class WriteFamilyTableTask(BaseDeleteTask):
    family_guid = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            family_table_path(
                self.reference_genome,
                self.dataset_type,
                self.family_guid,
            ),
        )
