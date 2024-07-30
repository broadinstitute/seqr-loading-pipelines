import hail as hl
import luigi

from v03_pipeline.lib.annotations.enums import SV_TYPES
from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.paths import variant_annotations_vcf_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.base.task import BaseTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


@luigi.util.inherits(BaseLoadingRunParams)
class WriteVariantAnnotationsVCF(BaseTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_vcf_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> bool:
        if not self.dataset_type.should_export_to_vcf:
            return True
        return self.output().exists()

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
        ht = ht.annotate(
            info=hl.Struct(
                ALGORITHMS=ht.algorithms,
                END=ht.locus.position,
                CHR2=ht.end_locus.contig,
                END2=ht.end_locus.position,
                SVTYPE=hl.array(SV_TYPES)[ht.sv_type_id],
                SVLEN=123,
            ),
        )
        hl.export_vcf(ht, self.output().path, tabix=True)
