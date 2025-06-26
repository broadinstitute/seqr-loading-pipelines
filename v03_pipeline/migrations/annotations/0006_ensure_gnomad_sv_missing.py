import hail as hl

from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, ReferenceGenome


class EnsureGnomadSVMissing(BaseMigration):
    reference_genome_dataset_types: frozenset[tuple[ReferenceGenome, DatasetType]] = (
        frozenset(
            ((ReferenceGenome.GRCh38, DatasetType.SV),),
        )
    )

    @staticmethod
    def migrate(ht: hl.Table, **_) -> hl.Table:
        return ht.annotate(
            gnomad_svs=hl.or_missing(
                hl.any([hl.is_defined(x) for x in ht.gnomad_svs.values()]),
                ht.gnomad_svs,
            ),
        )
