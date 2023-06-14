from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.model import AnnotationType
from v03_pipeline.lib.paths import (
    valid_reference_dataset_collection_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import (
    GCSorLocalFolderTarget,
    GCSorLocalTarget,
    HailTableTask,
)


class BaseVariantAnnotationsTableTask(BasePipelineTask):
    liftover_ref_path = luigi.OptionalParameter(
        default='gs://hail-common/references/grch38_to_grch37.over.chain.gz',
        description='Path to GRCh38 to GRCh37 coordinates file',
    )

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_table_path(
                self.env,
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists()

    def requires(self) -> list[luigi.Task]:
        requirements = [
            HailTableTask(
                valid_reference_dataset_collection_path(
                    self.env,
                    self.reference_genome,
                    self.dataset_type.base_reference_dataset_collection,
                ),
            ),
        ]
        for rdc in self.dataset_type.annotatable_reference_dataset_collections(
            self.env,
        ):
            requirements.append(
                HailTableTask(
                    valid_reference_dataset_collection_path(
                        self.env,
                        self.reference_genome,
                        rdc,
                    ),
                ),
            )
        return requirements

    def initialize_table(self) -> hl.Table:
        if self.dataset_type.base_reference_dataset_collection is None:
            key_type = self.dataset_type.table_key_type(self.reference_genome)
            ht = hl.Table.parallelize(
                [],
                key_type,
                key=key_type.fields,
            )
        else:
            ht = hl.read_table(
                valid_reference_dataset_collection_path(
                    self.env,
                    self.reference_genome,
                    self.dataset_type.base_reference_dataset_collection,
                ),
            )
            ht = ht.annotate(
                **get_fields(
                    ht,
                    AnnotationType.REFERENCE_DATASET_COLLECTION,
                    **self.param_kwargs,
                ),
                # NB: We will endeavor to remove the below line by calling running
                # the formatting annotations over the base reference dataset collection
                # itself when it is created.
                **get_fields(ht, AnnotationType.FORMATTING, **self.param_kwargs),
            )
        return ht.annotate_globals(
            updates=hl.empty_set(hl.tstruct(callset=hl.tstr, project_guid=hl.tstr)),
        )

    def update(self, ht: hl.Table) -> hl.Table:
        return ht
