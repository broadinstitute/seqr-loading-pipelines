from typing import ClassVar

import hail as hl
import luigi

from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.reference_data.compare_globals import (
    Globals,
    get_datasets_to_update,
)
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)


class UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
    BaseVariantAnnotationsTableTask,
):
    rdc = luigi.EnumParameter(enum=ReferenceDatasetCollection)
    _datasets_to_update: ClassVar[list[str]] = []

    def complete(self) -> bool:
        self._datasets_to_update.clear()

        if not super().complete():
            self._datasets_to_update.extend(
                self.rdc.datasets(
                    self.dataset_type,
                ),
            )
            return False

        annotations_ht_globals = Globals.from_ht(
            hl.read_table(self.output().path),
            self.rdc,
            self.dataset_type,
        )
        rdc_ht_globals = Globals.from_ht(
            self.rdc_annotation_dependencies[f'{self.rdc.value}_ht'],
            self.rdc,
            self.dataset_type,
        )
        self._datasets_to_update.extend(
            get_datasets_to_update(
                self.rdc,
                annotations_ht_globals,
                rdc_ht_globals,
                self.dataset_type,
            ),
        )
        return not self._datasets_to_update

    def update_table(self, ht: hl.Table) -> hl.Table:
        rdc_ht = self.rdc_annotation_dependencies[f'{self.rdc.value}_ht']

        for dataset in self._datasets_to_update:
            if dataset in ht.row:
                ht = ht.drop(dataset)

            ht = ht.join(rdc_ht.select(dataset), 'left')

        return self.fix_globals(ht, self.rdc)
