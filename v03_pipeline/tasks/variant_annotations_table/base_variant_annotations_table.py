import os

import luigi

from v03_pipeline.core.definitions import DatasetType, Env, ReferenceGenome, SampleType
from v03_pipeline.core.paths import variant_annotations_table_path
from v03_pipeline.tasks.files import GCSorLocalTarget


class BaseVariantAnnotationsTable(luigi.Task):
    env = luigi.EnumParameter(enum=Env)
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    sample_type = luigi.EnumParameter(enum=SampleType)

    @property
    def path(self) -> str:
        return variant_annotations_table_path(
            self.env,
            self.reference_genome,
            self.dataset_type,
        )

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self.path)

    def complete(self) -> bool:
        return GCSorLocalTarget(os.path.join(self.path, '_SUCCESS')).exists()

    def run(self) -> None:
        raise NotImplementedError
