import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDataset,
)
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingPipelineParams)
class UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
    BaseUpdateVariantAnnotationsTableTask,
):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._datasets_to_update: set[str] = set()

    def complete(self) -> bool:
        reference_dataset_names = {
            rd.name
            for rd in BaseReferenceDataset.for_reference_genome_dataset_type_annotations_updates(
                self.reference_genome,
                self.dataset_type,
            )
        }
        if not super().complete():
            self._datasets_to_update = reference_dataset_names
            return False
        # Find datasets with mismatched versions
        annotation_ht_versions = {
            dataset_name: version
            for dataset_name, version in hl.eval(
                hl.read_table(self.output().path).globals.versions,
            ).items()
            if not ReferenceDataset(dataset_name).exclude_from_annotations_updates
        }
        self._datasets_to_update = (
            reference_dataset_names ^ annotation_ht_versions.keys()
        )
        for dataset_name in reference_dataset_names & annotation_ht_versions.keys():
            if (
                ReferenceDataset(dataset_name).version(self.reference_genome)
                != annotation_ht_versions[dataset_name]
            ):
                self._datasets_to_update.add(dataset_name)
        logger.info(
            f'Datasets to update: {", ".join(d for d in self._datasets_to_update)}',
        )
        return not self._datasets_to_update

    def update_table(self, ht: hl.Table) -> hl.Table:
        for dataset_name in self._datasets_to_update:
            if dataset_name in ht.row:
                ht = ht.drop(dataset_name)
            if dataset_name not in set(ReferenceDataset):
                continue
            reference_dataset = ReferenceDataset(dataset_name)
            reference_dataset_ht = hl.read_table(
                valid_reference_dataset_path(self.reference_genome, reference_dataset),
            )
            if reference_dataset.formatting_annotation:
                ht = ht.annotate(
                    **get_fields(
                        ht,
                        [reference_dataset.formatting_annotation],
                        **{f'{reference_dataset.name}_ht': reference_dataset_ht},
                        **self.param_kwargs,
                    ),
                )
            else:
                if reference_dataset.select:
                    reference_dataset_ht = reference_dataset.select(
                        self.reference_genome,
                        self.dataset_type,
                        reference_dataset_ht,
                    )
                if reference_dataset.filter:
                    reference_dataset_ht = reference_dataset.filter(
                        self.reference_genome,
                        self.dataset_type,
                        reference_dataset_ht,
                    )
                reference_dataset_ht = reference_dataset_ht.select(
                    **{
                        f'{reference_dataset.name}': hl.Struct(
                            **reference_dataset_ht.row_value,
                        ),
                    },
                )
                ht = ht.join(reference_dataset_ht, 'left')

        return self.annotate_globals(
            ht,
            BaseReferenceDataset.for_reference_genome_dataset_type_annotations_updates(
                self.reference_genome,
                self.dataset_type,
            ),
        )
