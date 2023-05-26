from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations import annotate_all
from v03_pipeline.lib.definitions import AccessControl, Env, SampleType
from v03_pipeline.lib.paths import (
    reference_dataset_collection_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import (
    GCSorLocalFolderTarget,
    GCSorLocalTarget,
    HailTableTask,
)


class BaseVariantAnnotationsTableTask(BasePipelineTask):
    sample_type = luigi.EnumParameter(enum=SampleType)

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
        requirements = []
        if self.dataset_type.base_reference_dataset_collection:
            requirements.append(
                HailTableTask(
                    reference_dataset_collection_path(
                        self.env,
                        self.reference_genome,
                        self.dataset_type.base_reference_dataset_collection,
                    ),
                ),
            )
        for rdc in self.dataset_type.supplemental_reference_dataset_collections:
            if self.env == Env.LOCAL and rdc.access_control == AccessControl.PRIVATE:
                continue
            requirements.append(
                HailTableTask(
                    reference_dataset_collection_path(
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
                reference_dataset_collection_path(
                    self.env,
                    self.reference_genome,
                    self.dataset_type.base_reference_dataset_collection,
                ),
            )
        ht = annotate_all(ht, **self.param_kwargs)
        return ht.annotate_globals(
            updates=hl.empty_set(hl.ttuple(hl.tstr, hl.tstr)),
        )

    def update(self, mt: hl.Table) -> hl.Table:
        return mt
