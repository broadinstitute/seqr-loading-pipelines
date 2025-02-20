import hail as hl

from v03_pipeline.lib.annotations import sv
from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset

# This vcf was generated with the gatk command
PHASE_4_CALLSET_WITH_GNOMAD_V4 = 'gs://seqr-loading-temp/phase4.seqr.gnomad_v4.vcf.gz'


class AddGnomadSVs(BaseMigration):
    reference_genome_dataset_types: frozenset[
        tuple[ReferenceGenome, DatasetType]
    ] = frozenset(
        ((ReferenceGenome.GRCh38, DatasetType.SV),),
    )

    @staticmethod
    def migrate(ht: hl.Table, **_) -> hl.Table:
        mapping_ht = hl.import_vcf(
            PHASE_4_CALLSET_WITH_GNOMAD_V4,
            reference_genome=ReferenceGenome.GRCh38.value,
            force_bgz=True
        ).annotate_rows(variant_id=mt.rsid).key_rows_by(mt.variant_id).rows()
        ht = ht.annotate(
            **{
                'info.GNOMAD_V4.1_TRUTH_VID': mapping_ht[ht.key][
                    'info.GNOMAD_V4.1_TRUTH_VID'
                ],
            },
        )
        gnomad_svs_ht = ReferenceDataset.gnomad_svs.get_ht(ReferenceGenome.GRCh38)
        ht = ht.annotate(gnomad_svs=sv.gnomad_svs(ht, gnomad_svs_ht))
        ht = ht.drop('info.GNOMAD_V4.1_TRUTH_VID')
        return ht.annotate_globals(
            versions=ht.globals.versions.annotate(gnomad_svs='1.0'),
            enums=ht.globals.enums.annotate(gnomad_svs=hl.Struct()),
        )
