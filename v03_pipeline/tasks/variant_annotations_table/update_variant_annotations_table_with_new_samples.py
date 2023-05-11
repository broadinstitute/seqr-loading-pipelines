import hail as hl
import luigi

from v03_pipeline.core.paths import project_pedigree_path, project_subset_path
from v03_pipeline.tasks.files import RawFile, VCFFile
from v03_pipeline.tasks.variant_annotations_table.base_variant_annotations_table import (
    BaseVariantAnnotationsTable,
)


class UpdateVariantAnnotationsTableWithNewProject(BaseVariantAnnotationsTable):
    project_guid = luigi.Parameter('Project GUID')
    vcf_file = luigi.Parameter(
        description='Path to the vcf containing the new samples.',
    )

    def requires(self) -> luigi.Task:
        return [
            RawFile(
                project_subset_path(
                    self.reference_genome,
                    self.sample_source,
                    self.sample_type,
                    self.project_guid,
                ),
            ),
            RawFile(
                project_pedigree_path(
                    self.reference_genome,
                    self.sample_source,
                    self.sample_type,
                    self.project_guid,
                ),
            ),
            VCFFile(self.vcf_file),
        ]

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.read_table(
                self.variant_annotations_table_path,
            ).globals.sample_vcfs.contains(self.vcf_file),
        )

    def run(self) -> None:
        print('Running UpdateVariantAnnotationsTableWithNewSamples')
