import hail as hl

from v03_pipeline.lib.annotations import snv_indel
from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, ReferenceGenome


class AddRG38Locus(BaseMigration):
    reference_genome_dataset_types: frozenset[tuple[ReferenceGenome, DatasetType]] = (
        frozenset(
            ((ReferenceGenome.GRCh37, DatasetType.SNV_INDEL),),
        )
    )

    @staticmethod
    def migrate(ht: hl.Table, **_) -> hl.Table:
        return ht.annotate(
            rg38_locus=snv_indel.rg38_locus(
                ht,
            ),
        )
