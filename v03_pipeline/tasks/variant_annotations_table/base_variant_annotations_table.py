import os

import luigi

from v03_pipeline.core.definitions import (
    DatasetType,
    Env,
    ReferenceGenome,
    SampleSource,
    SampleType,
)
from v03_pipeline.core.paths import variant_annotations_table_path
from v03_pipeline.tasks.files import GCSorLocalTarget


class BaseVariantAnnotationsTable(luigi.Task):
    env = luigi.EnumParameter(enum=Env, default=Env.LOCAL)
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome, default=ReferenceGenome.GRCh38)
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    sample_type = luigi.EnumParameter(enum=SampleType)
    sample_source = luigi.EnumParameter(enum=SampleSource, default=SampleSource.LOCAL)

    @property
    def variant_annotations_table_path(self) -> str:
        return variant_annotations_table_path(
            self.env,
            self.reference_genome,
            self.dataset_type,
        )

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self.variant_annotations_table_path)

    def complete(self) -> bool:
        return GCSorLocalTarget(
            os.path.join(self.variant_annotations_table_path, '_SUCCESS'),
        ).exists()

    def run(self) -> None:
        raise NotImplementedError
