from typing import List

import hail as hl
import luigi

from v03_pipeline.tasks.files import RawFile
from v03_pipeline.tasks.variant_annotations_table.base_variant_annotations_table import (
    BaseVariantAnnotationsTable,
)


class UpdateVariantAnnotationsTableWithNewSamples(BaseVariantAnnotationsTable):
    vcf_file = luigi.Parameter(
        description='Path to the vcf containing the new samples.',
    )

    def requires(self) -> List[luigi.Task]:
        return [
            RawFile(self.vcf_file),
        ]

    def complete(self) -> None:
        return super().complete() and hl.eval(
            hl.read_table(self.path).globals.sample_vcfs.contains(self.vcf_file),
        )
