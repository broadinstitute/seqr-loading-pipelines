import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.core import (
    Env,
)
from v03_pipeline.lib.misc.allele_registry import register_alleles_in_chunks
from v03_pipeline.lib.misc.io import checkpoint
from v03_pipeline.lib.paths import (
    new_variants_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_new_variants_table import WriteNewVariantsTableTask


@luigi.util.inherits(BaseLoadingRunParams)
class UpdateNewVariantsWithCAIDsTask(BaseUpdateTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            new_variants_table_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return [
            self.clone(WriteNewVariantsTableTask),
        ]

    def complete(self) -> bool:
        return super().complete() and hasattr(hl.read_table(self.output().path), 'CAID')

    def update_table(self, ht: hl.Table) -> hl.Table:
        # Register the new variant alleles to the Clingen Allele Registry
        # and annotate new_variants table with CAID.
        if not (
            Env.CLINGEN_ALLELE_REGISTRY_LOGIN and Env.CLINGEN_ALLELE_REGISTRY_PASSWORD
        ):
            return ht.annotate(CAID=hl.missing(hl.tstr))
        ar_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus(self.reference_genome.value),
                alleles=hl.tarray(hl.tstr),
                CAID=hl.tstr,
            ),
            key=('locus', 'alleles'),
        )
        for ar_ht_chunk in register_alleles_in_chunks(
            ht,
            self.reference_genome,
        ):
            ar_ht = ar_ht.union(ar_ht_chunk)
            ar_ht, _ = checkpoint(ar_ht)
        return ht.join(ar_ht, 'left')
