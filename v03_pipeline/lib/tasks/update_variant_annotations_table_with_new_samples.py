from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.definitions import SampleType
from v03_pipeline.lib.misc.io import import_pedigree, import_vcf
from v03_pipeline.lib.misc.pedigree import samples_to_include
from v03_pipeline.lib.misc.remap import remap_sample_ids
from v03_pipeline.lib.misc.subset import subset_samples_and_variants
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, HailTable, VCFFile


class UpdateVariantAnnotationsTableWithNewSamples(BaseVariantAnnotationsTableTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    vcf_path = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()

    dont_validate = luigi.BoolParameter(
        description='Disable checking whether the dataset matches the specified sample type and genome version',
    )
    ignore_missing_samples = luigi.BoolParameter()
    vep_config_json_path = luigi.OptionalParameter(
        default=None,
        description='Path of hail vep config .json file',
    )
    grch38_to_grch37_ref_chain = luigi.OptionalParameter(
        default='gs://hail-common/references/grch38_to_grch37.over.chain.gz',
        description='Path to GRCh38 to GRCh37 coordinates file',
    )

    def requires(self) -> list[luigi.Task]:
        return [
            VCFFile(self.vcf_path),
            HailTable(self.project_remap_path),
            HailTable(self.project_pedigree_path),
        ]

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists()

    def update(self, existing_mt: hl.MatrixTable) -> hl.MatrixTable:
        vcf_mt = import_vcf(self.vcf_path, self.reference_genome)
        project_remap_ht = hl.import_table(self.project_remap_path)
        vcf_mt = remap_sample_ids(vcf_mt, project_remap_ht)

        pedigree_ht = import_pedigree(self.project_pedigree_path)
        sample_subset_ht = samples_to_include(pedigree_ht, vcf_mt.cols())
        vcf_mt = subset_samples_and_variants(
            vcf_mt,
            sample_subset_ht,
            self.ignore_missing_samples,
        )
