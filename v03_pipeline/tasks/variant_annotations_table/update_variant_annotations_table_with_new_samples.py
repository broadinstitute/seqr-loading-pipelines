import os
from typing import List

import hail as hl
import luigi

from v03_pipeline.core.definitions import DatasetType, Env, ReferenceGenome, SampleType
from v03_pipeline.tasks.files import RawFile


class UpdateVariantAnnotationsTableWithNewSamples(BaseVariantAnnotationsTable):
    new_vcf = luigi.Parameter(
        description='Path to the vcf containing the new samples.',
    )

    def requires(self) -> List[luigi.Task]:
        return [
            RawFile(self.new_sample_vcf),
        ]

    def complete(self) -> None:
        return super().complete() and hl.eval(
            hl.read_table(self.path).globals.sample_vcfs.contains(self.new_vcf),
        )
