import hail as hl

from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, ReferenceGenome


class AddKeyField(BaseMigration):
    reference_genome_dataset_types: frozenset[tuple[ReferenceGenome, DatasetType]] = (
        frozenset(
            (
                (ReferenceGenome.GRCh37, DatasetType.SNV_INDEL),
                (ReferenceGenome.GRCh38, DatasetType.SNV_INDEL),
                (ReferenceGenome.GRCh38, DatasetType.MITO),
                (ReferenceGenome.GRCh38, DatasetType.GCNV),
                (ReferenceGenome.GRCh38, DatasetType.SV),
            ),
        )
    )

    @staticmethod
    def migrate(ht: hl.Table, **_) -> hl.Table:
        ht = ht.add_index(name='key_')
        return ht.annotate_globals(max_seen_id=(ht.count() - 1))
