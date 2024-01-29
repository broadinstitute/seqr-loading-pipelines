from typing import ClassVar

import hail as hl
import luigi

from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.reference_data.compare_globals import (
    Globals,
    GlobalsValidator,
)
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)


class UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
    BaseVariantAnnotationsTableTask,
):
    rdc = luigi.EnumParameter(enum=ReferenceDatasetCollection)
    _rdc_datasets_to_update: ClassVar[list[str]] = []

    def complete(self) -> bool:
        annotations_ht_globals = Globals.from_ht(
            hl.read_table(self.output().path),
            self.rdc,
            self.dataset_type,
        )
        rdc_ht_globals = Globals.from_ht(
            self.annotation_dependencies[f'{self.rdc.value}_ht'],
            self.rdc,
            self.dataset_type,
        )
        self._rdc_datasets_to_update.extend(
            GlobalsValidator(
                annotations_ht_globals,
                rdc_ht_globals,
                self.rdfc,
                self.dataset_type,
            ).get_datasets_to_update(),
        )
        return not self._rdc_datasets_to_update

    def update_table(self, ht: hl.Table) -> hl.Table:
        rdc_ht = self.annotation_dependencies[f'{self.rdc.value}_ht']
        for dataset in self.rdc.datasets(self.dataset_type):
            ht = ht.drop(dataset)
        return ht.join(rdc_ht, 'outer')
