from collections.abc import Callable
from enum import Enum

import hail as hl

from v03_pipeline.lib.annotations import gcnv, mito, shared, snv_indel, sv
from v03_pipeline.lib.model.definitions import ReferenceGenome

MITO_MIN_HOM_THRESHOLD = 0.95
ZERO = 0.0


class DatasetType(Enum):
    GCNV = 'GCNV'
    MITO = 'MITO'
    SNV_INDEL = 'SNV_INDEL'
    SV = 'SV'

    def table_key_type(
        self,
        reference_genome: ReferenceGenome,
    ) -> hl.tstruct:
        default_key = hl.tstruct(
            locus=hl.tlocus(reference_genome.value),
            alleles=hl.tarray(hl.tstr),
        )
        return {
            DatasetType.GCNV: hl.tstruct(variant_id=hl.tstr),
            DatasetType.SV: hl.tstruct(variant_id=hl.tstr),
        }.get(self, default_key)

    @property
    def col_fields(
        self,
    ) -> list[str]:
        return {
            DatasetType.SNV_INDEL: [],
            DatasetType.MITO: ['contamination', 'mito_cn'],
            DatasetType.SV: [],
            DatasetType.GCNV: [],
        }[self]

    @property
    def entries_fields(
        self,
    ) -> list[str]:
        return {
            DatasetType.SNV_INDEL: ['GT', 'AD', 'GQ'],
            DatasetType.MITO: ['GT', 'DP', 'MQ', 'HL'],
            DatasetType.SV: ['GT', 'CONC_ST', 'GQ', 'RD_CN'],
            DatasetType.GCNV: [
                'any_ovl',
                'defragmented',
                'genes_any_overlap_Ensemble_ID',
                'genes_any_overlap_totalExons',
                'identical_ovl',
                'is_latest',
                'no_ovl',
                'sample_start',
                'sample_end',
                'CN',
                'GT',
                'QS',
            ],
        }[self]

    @property
    def row_fields(
        self,
    ) -> list[str]:
        return {
            DatasetType.SNV_INDEL: ['rsid', 'filters'],
            DatasetType.MITO: [
                'rsid',
                'filters',
                'common_low_heteroplasmy',
                'hap_defining_variant',
                'mitotip_trna_prediction',
                'vep.transcript_consequences',
                'vep.most_severe_consequence',
            ],
            DatasetType.SV: [
                'locus',
                'alleles',
                'filters',
                'info.AC',
                'info.AF',
                'info.ALGORITHMS',
                'info.AN',
                'info.CHR2',
                'info.CPX_INTERVALS',
                'info.CPX_TYPE',
                'info.END',
                'info.END2',
                'info.gnomAD_V2_AF',
                'info.gnomAD_V2_SVID',
                'info.N_HET',
                'info.N_HOMALT',
                'info.StrVCTVRE',
            ],
            DatasetType.GCNV: [
                'cg_genes',
                'chr',
                'end',
                'filters',
                'gene_ids',
                'lof_genes',
                'num_exon',
                'sc',
                'sf',
                'start',
                'strvctvre_score',
                'svtype',
            ],
        }[self]

    @property
    def excluded_filters(self) -> hl.SetExpression:
        return {
            DatasetType.SNV_INDEL: hl.empty_set(hl.tstr),
            DatasetType.MITO: hl.set(['PASS']),
            DatasetType.SV: hl.set(['PASS', 'BOTHSIDES_SUPPORT']),
            DatasetType.GCNV: hl.empty_set(hl.tstr),
        }[self]

    @property
    def has_lookup_table(self) -> bool:
        return self in {DatasetType.SNV_INDEL, DatasetType.MITO}

    @property
    def has_gencode_mapping(self) -> dict[str, str]:
        return self == DatasetType.SV

    @property
    def has_multi_allelic_variants(self) -> bool:
        return self == DatasetType.SNV_INDEL

    @property
    def family_entries_filter_fn(self) -> Callable[[hl.StructExpression], bool]:
        return {
            DatasetType.GCNV: lambda e: hl.is_defined(e.GT),
        }.get(self, lambda e: e.GT.is_non_ref())

    @property
    def can_run_validation(self) -> bool:
        return self == DatasetType.SNV_INDEL

    @property
    def check_sex_and_relatedness(self) -> bool:
        return self == DatasetType.SNV_INDEL

    @property
    def veppable(self) -> bool:
        return self == DatasetType.SNV_INDEL

    @property
    def lookup_table_fields_and_genotype_filter_fns(
        self,
    ) -> dict[str, Callable[[hl.StructExpression], hl.Expression]]:
        return {
            DatasetType.SNV_INDEL: {
                'ref_samples': lambda s: s.GT.is_hom_ref(),
                'het_samples': lambda s: s.GT.is_het(),
                'hom_samples': lambda s: s.GT.is_hom_var(),
            },
            DatasetType.MITO: {
                'ref_samples': lambda s: hl.is_defined(s.HL) & (s.HL == ZERO),
                'heteroplasmic_samples': lambda s: (
                    (s.HL < MITO_MIN_HOM_THRESHOLD) & (s.HL > ZERO)
                ),
                'homoplasmic_samples': lambda s: s.HL >= MITO_MIN_HOM_THRESHOLD,
            },
        }[self]

    def formatting_annotation_fns(
        self,
        reference_genome: ReferenceGenome,
    ) -> list[Callable[..., hl.Expression]]:
        GRCh37_fns = {  # noqa: N806
            DatasetType.SNV_INDEL: [
                shared.rsid,
                shared.sorted_transcript_consequences,
                shared.variant_id,
                shared.xpos,
            ],
            DatasetType.MITO: [
                mito.common_low_heteroplasmy,
                mito.haplogroup,
                mito.high_constraint_region_mito,
                mito.mitotip,
                mito.rsid,
                shared.sorted_transcript_consequences,
                shared.variant_id,
                shared.xpos,
            ],
            DatasetType.SV: [
                sv.algorithms,
                sv.bothsides_support,
                sv.cpx_intervals,
                sv.end_locus,
                sv.gt_stats,
                sv.gnomad_svs,
                sv.sorted_gene_consequences,
                sv.start_locus,
                sv.strvctvre,
                sv.sv_type_id,
                sv.sv_type_detail_id,
                shared.xpos,
            ],
            DatasetType.GCNV: [
                gcnv.end_locus,
                gcnv.gt_stats,
                gcnv.num_exon,
                gcnv.sorted_gene_consequences,
                gcnv.start_locus,
                gcnv.strvctvre,
                gcnv.sv_type_id,
                gcnv.xpos,
            ],
        }
        if reference_genome == ReferenceGenome.GRCh37:
            return GRCh37_fns[self]
        return {
            DatasetType.SNV_INDEL: [
                *GRCh37_fns[DatasetType.SNV_INDEL],
                snv_indel.gnomad_non_coding_constraint,
                snv_indel.screen,
                shared.rg37_locus,
            ],
            DatasetType.MITO: [
                *GRCh37_fns[DatasetType.MITO],
                shared.rg37_locus,
            ],
            DatasetType.SV: [
                *GRCh37_fns[DatasetType.SV],
                shared.rg37_locus,
                sv.rg37_locus_end,
            ],
            DatasetType.GCNV: [
                *GRCh37_fns[DatasetType.GCNV],
                gcnv.rg37_locus,
                gcnv.rg37_locus_end,
            ],
        }[self]

    @property
    def genotype_entry_annotation_fns(self) -> list[Callable[..., hl.Expression]]:
        return {
            DatasetType.SNV_INDEL: [
                shared.GQ,
                snv_indel.AB,
                snv_indel.DP,
                shared.GT,
            ],
            DatasetType.MITO: [
                mito.contamination,
                mito.DP,
                mito.HL,
                mito.mito_cn,
                mito.GQ,
                shared.GT,
            ],
            DatasetType.SV: [
                sv.CN,
                sv.concordance,
                shared.GQ,
                shared.GT,
            ],
            DatasetType.GCNV: [
                gcnv.concordance,
                gcnv.defragged,
                gcnv.sample_end,
                gcnv.sample_gene_ids,
                gcnv.sample_num_exon,
                gcnv.sample_start,
                gcnv.CN,
                gcnv.GT,
                gcnv.QS,
            ],
        }[self]

    @property
    def lookup_table_annotation_fns(self) -> list[Callable[..., hl.Expression]]:
        return {
            DatasetType.SNV_INDEL: [
                snv_indel.gt_stats,
            ],
            DatasetType.MITO: [
                mito.gt_stats,
            ],
        }[self]

    @property
    def should_send_to_allele_registry(self):
        return self == DatasetType.SNV_INDEL
