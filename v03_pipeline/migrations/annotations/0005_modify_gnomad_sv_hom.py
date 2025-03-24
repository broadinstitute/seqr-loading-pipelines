import hail as hl

from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, ReferenceGenome

GNOMAD_SVS_HT = 'gs://seqr-reference-data/v3.1/GRCh38/gnomad_svs/1.1.ht'


class ModifyGnomadSVHom(BaseMigration):
    reference_genome_dataset_types: frozenset[
        tuple[ReferenceGenome, DatasetType]
    ] = frozenset(
        ((ReferenceGenome.GRCh38, DatasetType.SV),),
    )

    @staticmethod
    def migrate(ht: hl.Table, **_) -> hl.Table:
        gnomad_svs_ht = hl.read_table(GNOMAD_SVS_HT)
        ht = ht.annotate(
            gnomad_svs=hl.struct(
                AF=ht.gnomad_svs.AF,
                AC=ht.gnomad_svs.AC,
                AN=ht.gnomad_svs.AN,
                N_HET=ht.gnomad_svs.N_HET,
                N_HOM=gnomad_svs_ht[ht.gnomad_svs.ID].N_HOM,
                ID=ht.gnomad_svs.ID,
            ),
        )
        return ht.annotate_globals(
            versions=ht.globals.versions.annotate(gnomad_svs='1.1'),
        )
