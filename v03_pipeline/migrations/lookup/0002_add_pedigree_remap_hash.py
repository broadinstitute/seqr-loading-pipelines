import hail as hl

from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, ReferenceGenome


class RemoveNullFamilies(BaseMigration):
    @property
    def reference_genome_dataset_types() -> (
        frozenset[tuple[ReferenceGenome, DatasetType]]
    ):
        return frozenset(
            (
                (ReferenceGenome.GRCh37, DatasetType.SNV_INDEL),
                (ReferenceGenome.GRCh38, DatasetType.SNV_INDEL),
                (ReferenceGenome.GRCh38, DatasetType.MITO),
            ),
        )

    @staticmethod
    def migrate(ht: hl.Table) -> hl.Table:
        return ht.annotate_globals(
            updates=ht.globals.updates.map(
                lambda u: u.annotate(remap_pedigree_hash=hl.missing(hl.tint32)),
            ),
        )
