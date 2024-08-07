import hail as hl

from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, ReferenceGenome


class AddMigrationsGlobals(BaseMigration):
    @property
    def reference_genome_dataset_types() -> (
        frozenset[tuple[ReferenceGenome, DatasetType]]
    ):
        return frozenset(
            (
                (ReferenceGenome.GRCh37, DatasetType.SNV_INDEL),
                (ReferenceGenome.GRCh38, DatasetType.SNV_INDEL),
                (ReferenceGenome.GRCh38, DatasetType.MITO),
                (ReferenceGenome.GRCh38, DatasetType.GCNV),
                (ReferenceGenome.GRCh38, DatasetType.SV),
            ),
        )

    @staticmethod
    def migrate(ht: hl.Table) -> hl.Table:
        return ht.annotate_globals(migrations=hl.empty_list(hl.str))
