import hail as hl
import luigi

from v03_pipeline.lib.tasks.base.base_variant_annotations_table import BaseVariantAnnotationsTableTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget, HailTable, VCFFile


class UpdateVariantAnnotationsTableWithNewSamples(BaseVariantAnnotationsTableTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    vcf_path = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_subset_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()

    dont_validate = luigi.BoolParameter(
        description='Disable checking whether the dataset matches the specified sample type and genome version')
    vep_config_json_path = luigi.OptionalParameter(
        default=None,
        description="Path of hail vep config .json file"
    )
    grch38_to_grch37_ref_chain = luigi.OptionalParameter(
        default='gs://hail-common/references/grch38_to_grch37.over.chain.gz',
        description="Path to GRCh38 to GRCh37 coordinates file"
    )

    def requires(self) -> list[luigi.Task]:
        return [
            VCFFile(self.vcf_path),
            HailTable(self.project_remap_path),
            HailTable(self.project_subset_path),
            HailTable(self.project_pedigree_path),
        ]


    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists()

    def update(self, mt: hl.MatrixTable) -> hl.MatrixTable:
        return mt
