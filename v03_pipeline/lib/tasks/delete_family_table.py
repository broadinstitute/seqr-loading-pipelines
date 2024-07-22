import luigi

from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.paths import family_table_path
from v03_pipeline.lib.tasks.base.base_delete_table import BaseDeleteTableTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class DeleteFamilyTableTask(BaseDeleteTableTask):
    family_guid = luigi.Parameter()
    sample_type = luigi.EnumParameter(enum=SampleType)

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            family_table_path(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.family_guid,
            ),
        )
