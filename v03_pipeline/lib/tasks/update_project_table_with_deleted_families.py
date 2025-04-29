import hail as hl
import luigi

from v03_pipeline.lib.misc.family_entries import (
    initialize_project_table,
    remove_family_guids,
)
from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class UpdateProjectTableWithDeletedFamiliesTask(BaseUpdateTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    project_guid = luigi.Parameter()
    family_guids = luigi.ListParameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            project_table_path(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.project_guid,
            ),
        )

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.bind(
                lambda family_guids: (
                    hl.all(
                        hl.array(list(self.family_guids)).map(
                            lambda family_guid: ~family_guids.contains(family_guid),
                        ),
                    )
                ),
                hl.set(
                    hl.read_table(self.output().path).globals.family_guids,
                ),
            ),
        )

    def initialize_table(self) -> hl.Table:
        return initialize_project_table(
            self.reference_genome,
            self.dataset_type,
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        return remove_family_guids(
            ht,
            hl.set(list(self.family_guids)),
        )
