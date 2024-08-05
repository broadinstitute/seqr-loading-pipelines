import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.paths import variant_annotations_vcf_path
from v03_pipeline.lib.tasks.base.base_hail_table import BaseHailTableTask
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


@luigi.util.inherits(BaseLoadingRunParams)
class WriteVariantAnnotationsVCF(BaseHailTableTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_vcf_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> bool:
        return not self.dataset_type.should_export_to_vcf

    def requires(self) -> luigi.Task:
        return self.clone(BaseUpdateVariantAnnotationsTableTask, force=False)

    def run(self) -> None:
        ht = hl.read_table(self.input().path)
        ht = ht.annotate(
            **get_fields(
                ht,
                self.dataset_type.export_vcf_annotation_fns,
                **self.param_kwargs,
            ),
        )
        ht = ht.key_by('locus', 'alleles')
        hl.export_vcf(ht, self.output().path, tabix=True)
