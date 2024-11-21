import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDataset,
    ReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingRunParams)
class UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
    BaseUpdateVariantAnnotationsTableTask,
):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._datasets_to_update: list[ReferenceDataset | ReferenceDatasetQuery] = []
        self._datasets_to_drop: list[str] = []

    def complete(self) -> bool:
        self._datasets_to_update = []
        self._datasets_to_drop = []

        reference_datasets = (
            BaseReferenceDataset.for_reference_genome_dataset_type_annotations(
                self.reference_genome,
                self.dataset_type,
            )
        )
        if not super().complete():
            self._datasets_to_update.extend(reference_datasets)
            return False

        annotation_ht_versions = dict(
            hl.eval(hl.read_table(self.output().path).globals.versions),
        )

        # Find datasets with mismatched versions
        for dataset in reference_datasets:
            if annotation_ht_versions.get(dataset.name) != dataset.version(
                self.reference_genome,
            ):
                self._datasets_to_update.append(dataset)

        if self._datasets_to_update:
            logger.info(
                f"Updating annotations for: {', '.join(d.name for d in self._datasets_to_update)}",
            )

        # Find datasets that are no longer valid and need to be dropped
        self._datasets_to_drop.extend(
            set(annotation_ht_versions.keys())
            - {dataset.name for dataset in reference_datasets},
        )
        return not self._datasets_to_update and not self._datasets_to_drop

    def update_table(self, ht: hl.Table) -> hl.Table:
        for dataset in self._datasets_to_drop:
            ht = ht.drop(dataset)

        for reference_dataset in self._datasets_to_update:
            if reference_dataset.name in ht.row:
                ht = ht.drop(reference_dataset.name)

            reference_dataset_ht = hl.read_table(
                valid_reference_dataset_path(self.reference_genome, reference_dataset),
            )
            if reference_dataset.is_keyed_by_interval:
                formatting_fn = next(
                    x
                    for x in self.dataset_type.formatting_annotation_fns(
                        self.reference_genome,
                    )
                    if x.__name__ == reference_dataset.name
                )
                ht = ht.annotate(
                    **get_fields(
                        ht,
                        [formatting_fn],
                        **{f'{reference_dataset.name}_ht': reference_dataset_ht},
                        **self.param_kwargs,
                    ),
                )
            else:
                reference_dataset_ht = reference_dataset_ht.select(
                    **{
                        f'{reference_dataset.name}': hl.Struct(
                            **reference_dataset_ht.row_value,
                        ),
                    },
                )
                ht = ht.join(reference_dataset_ht, 'left')

        return self.annotate_globals(ht)
