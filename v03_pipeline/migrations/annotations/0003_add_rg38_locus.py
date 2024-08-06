import hail as hl

from v03_pipeline.lib.annotations import snv_indel
from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome


class AddRG38Locus(BaseMigration):
    @property
    def reference_genome_dataset_types() -> (
        frozenset[tuple[ReferenceGenome, DatasetType]]
    ):
        return frozenset(
            ((ReferenceGenome.GRCh37, DatasetType.SNV_INDEL),),
        )

    @staticmethod
    def migrate(ht: hl.Table) -> hl.Table:
        return ht.annotate(
            rg38_locus=snv_indel.rg38_locus(ht, Env.GRCH37_TO_GRCH38_LIFTOVER_REF_PATH),
        )
