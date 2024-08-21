import hail as hl
import luigi

from v03_pipeline.lib.misc.lookup import (
    remove_family_guids,
)
from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.tasks.base.base_update_lookup_table import (
    BaseUpdateLookupTableTask,
)


class UpdateLookupTableWithDeletedFamiliesTask(BaseUpdateLookupTableTask):
    project_guid = luigi.Parameter()
    family_guids = luigi.ListParameter()

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.bind(
                lambda family_missing_for_project_sample_type: hl.all(
                    hl.array(family_missing_for_project_sample_type),
                ),
                hl.array([sample_type.value for sample_type in SampleType]).map(
                    lambda sample_type: hl.bind(
                        lambda family_guids: (
                            hl.is_missing(
                                family_guids,
                            )  # The project + sample_type pair is missing
                            | hl.all(
                                hl.array(list(self.family_guids)).map(
                                    lambda family_guid: ~family_guids.contains(
                                        family_guid,
                                    ),
                                ),
                            )
                        ),
                        hl.set(
                            hl.read_table(
                                self.output().path,
                            ).globals.project_families.get(
                                (self.project_guid, sample_type),
                            ),
                        ),
                    ),
                ),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        for sample_type in SampleType:
            ht = remove_family_guids(
                ht,
                self.project_guid,
                sample_type,
                hl.set(list(self.family_guids)),
            )
        return ht
