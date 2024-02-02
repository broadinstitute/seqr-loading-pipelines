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

    def complete(self) -> bool:
        if not super().complete():
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
        updated_datasets_for_rdc = get_datasets_to_update(
            self.rdc,
            annotations_ht_globals,
            rdc_ht_globals,
            self.dataset_type,
        )
        return len(updated_datasets_for_rdc) == 0

    def update_table(self, ht: hl.Table) -> hl.Table:
        rdc_ht = self.rdc_annotation_dependencies[f'{self.rdc.value}_ht']
        rdc_datasets = self.rdc.datasets(self.dataset_type)

        for dataset in rdc_datasets:
            if dataset in ht.row:
                ht = ht.drop(dataset)

        ht = ht.join(rdc_ht, 'left')
        return self.fix_globals(ht, self.rdc)
