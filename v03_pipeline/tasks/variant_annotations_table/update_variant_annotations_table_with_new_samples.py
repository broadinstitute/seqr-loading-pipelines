from typing import List

import hail as hl
import luigi

from v03_pipeline.tasks.files import VCFFile
from v03_pipeline.tasks.variant_annotations_table.base_variant_annotations_table import (
    BaseVariantAnnotationsTable,
)


class UpdateVariantAnnotationsTableWithNewSamples(BaseVariantAnnotationsTable):
    vcf_file = luigi.Parameter(
        description='Path to the vcf containing the new samples.',
    )
    vcf_remap_version = luigi.OptionalParameter(
        description='Path suffix used to find the vcf remap file',
    )

    def requires(self) -> luigi.Task:
        return VCFFile(self.vcf_file)

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.read_table(self.path).globals.sample_vcfs.contains(self.vcf_file),
        )

    def run(self) -> None:
        print("Running UpdateVariantAnnotationsTableWithNewSamples")
