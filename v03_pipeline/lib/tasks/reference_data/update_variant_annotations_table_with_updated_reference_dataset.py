import hail as hl

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.reference_data.compare_globals import (
    Globals,
    get_datasets_to_update,
)
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)

logger = get_logger(__name__)


class UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
    BaseVariantAnnotationsTableTask,
):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._datasets_to_update = []

    @property
    def reference_dataset_collections(self) -> list[ReferenceDatasetCollection]:
        return ReferenceDatasetCollection.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        )

    def complete(self) -> bool:
        logger.info(
            'Checking if UpdateVariantAnnotationsTableWithUpdatedReferenceDataset is complete',
        )
        self._datasets_to_update = []

        if not super().complete():
            for rdc in self.reference_dataset_collections:
                self._datasets_to_update.extend(
                    rdc.datasets(
                        self.dataset_type,
                    ),
                )
            return False

        for rdc in self.reference_dataset_collections:
            annotations_ht_globals = Globals.from_ht(
                hl.read_table(self.output().path),
                rdc.datasets(self.dataset_type),
            )
            dataset_config_globals = Globals.from_dataset_configs(
                self.reference_genome,
                rdc.datasets(self.dataset_type),
            )
            self._datasets_to_update.extend(
                get_datasets_to_update(
                    rdc,
                    annotations_ht_globals,
                    dataset_config_globals,
                    self.dataset_type,
                ),
            )
        logger.info(f'Datasets to update: {self._datasets_to_update}')
        return not self._datasets_to_update

    def update_table(self, ht: hl.Table) -> hl.Table:
        for dataset in self._datasets_to_update:
            rdc = ReferenceDatasetCollection.for_dataset(dataset, self.dataset_type)
            rdc_ht = self.rdc_annotation_dependencies[f'{rdc.value}_ht']
            if dataset in ht.row:
                ht = ht.drop(dataset)
            if rdc.requires_annotation:
                formatting_fn = next(
                    x
                    for x in self.dataset_type.formatting_annotation_fns(
                        self.reference_genome,
                    )
                    if x.__name__ == dataset
                )
                ht = ht.annotate(
                    **get_fields(
                        ht,
                        [formatting_fn],
                        **self.rdc_annotation_dependencies,
                        **self.param_kwargs,
                    ),
                )
            else:
                ht = ht.join(rdc_ht.select(dataset), 'left')
        return self.annotate_globals(ht)
