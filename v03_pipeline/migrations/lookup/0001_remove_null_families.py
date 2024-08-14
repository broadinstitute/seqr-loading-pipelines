import hail as hl

from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, ReferenceGenome


class RemoveNullFamilies(BaseMigration):
    reference_genome_dataset_types: frozenset[
        tuple[ReferenceGenome, DatasetType]
    ] = frozenset(
        (
            (ReferenceGenome.GRCh37, DatasetType.SNV_INDEL),
            (ReferenceGenome.GRCh38, DatasetType.SNV_INDEL),
            (ReferenceGenome.GRCh38, DatasetType.MITO),
        ),
    )

    @staticmethod
    def migrate(ht: hl.Table) -> hl.Table:
        ht = ht.annotate(
            project_stats=ht.project_stats.map(
                lambda ps: hl.or_missing(hl.any(ps.map(hl.is_defined)), ps),
            ),
        )
        return ht.annotate_globals(migrations=hl.empty_array(hl.tstr))
