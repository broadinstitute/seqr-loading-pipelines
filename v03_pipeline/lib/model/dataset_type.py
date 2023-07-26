from __future__ import annotations

from enum import Enum
from typing import Callable

import hail as hl

from hail_scripts.utils.mapping_gene_ids import load_gencode

from v03_pipeline.lib.annotations import (
    gcnv,
    mito,
    sample_lookup_table,
    shared,
    snv,
    sv,
)
from v03_pipeline.lib.model.definitions import AccessControl, Env, ReferenceGenome
from v03_pipeline.lib.model.reference_dataset_collection import (
    ReferenceDatasetCollection,
)

GENCODE_RELEASE = 42


class DatasetType(Enum):
    GCNV = 'GCNV'
    MITO = 'MITO'
    SNV = 'SNV'
    SV = 'SV'

    @property
    def annotatable_reference_dataset_collections(
        self,
    ) -> list[ReferenceDatasetCollection]:
        return {
            DatasetType.SNV: [
                ReferenceDatasetCollection.INTERVAL,
            ],
            DatasetType.MITO: [
                ReferenceDatasetCollection.INTERVAL_MITO,
            ],
        }.get(self, [])

    def joinable_reference_dataset_collections(
        self,
        env: Env,
    ) -> list[ReferenceDatasetCollection]:
        rdcs = {
            DatasetType.SNV: [
                ReferenceDatasetCollection.COMBINED,
                ReferenceDatasetCollection.HGMD,
            ],
            DatasetType.MITO: [
                ReferenceDatasetCollection.COMBINED_MITO,
            ],
        }.get(self, [])
        if env == Env.LOCAL:
            return [rdc for rdc in rdcs if rdc.access_control == AccessControl.PUBLIC]
        return rdcs

    def table_key_type(
        self,
        reference_genome: ReferenceGenome,
    ) -> hl.tstruct:
        default_key = hl.tstruct(
            locus=hl.tlocus(reference_genome.value),
            alleles=hl.tarray(hl.tstr),
        )
        return {
            DatasetType.GCNV: hl.tstruct(variant_name=hl.tstr, svtype=hl.tstr),
            DatasetType.SV: hl.tstruct(rsid=hl.tstr),
        }.get(self, default_key)

    @property
    def col_fields(
        self,
    ) -> list[str]:
        return {
            DatasetType.SNV: [],
            DatasetType.MITO: ['contamination', 'mito_cn'],
            DatasetType.SV: [],
        }[self]

    @property
    def entries_fields(
        self,
    ) -> list[str]:
        return {
            DatasetType.SNV: ['GT', 'AD', 'GQ'],
            DatasetType.MITO: ['GT', 'DP', 'MQ', 'HL'],
            DatasetType.SV: [],
        }[self]

    @property
    def row_fields(
        self,
    ) -> list[str]:
        return {
            DatasetType.SNV: ['rsid', 'filters'],
            DatasetType.MITO: [
                'rsid',
                'filters',
                'common_low_heteroplasmy',
                'hap_defining_variant',
                'AF_het',
                'AC_het',
                'AN',
                'mitotip_trna_prediction',
                'vep',
            ],
            DatasetType.SV: ['locus', 'alleles', 'filters', 'info'],
        }[self]

    @property
    def has_sample_lookup_table(self) -> bool:
        return self in {DatasetType.SNV, DatasetType.MITO}

    @property
    def has_gencode_mapping(self) -> dict[str, str]:
        return self == DatasetType.SV

    @property
    def veppable(self) -> bool:
        return self == DatasetType.SNV

    @property
    def formatting_annotation_fns(self) -> list[Callable[..., hl.Expression]]:
        return {
            DatasetType.SNV: [
                snv.gnomad_non_coding_constraint,
                snv.screen,
                shared.rg37_locus,
                shared.rsid,
                shared.sorted_transcript_consequences,
                shared.variant_id,
                shared.xpos,
            ],
            DatasetType.MITO: [
                mito.common_low_heteroplasmy,
                mito.callset_heteroplasmy,
                mito.haplogroup,
                mito.high_constraint_region,
                mito.mitotip,
                shared.rg37_locus,
                mito.rsid,
                shared.sorted_transcript_consequences,
                shared.variant_id,
                shared.xpos,
            ],
            DatasetType.SV: [
                sv.algorithms,
                sv.bothsides_support,
                sv.cpx_intervals,
                sv.filters,
                sv.gt_stats,
                sv.gnomad_svs,
                shared.rg37_locus,
                sv.rg37_locus_end,
                sv.sorted_gene_consequences,
                sv.strvctvre,
                sv.sv_type_id,
                sv.sv_type_detail_id,
                shared.xpos,
            ],
            DatasetType.GCNV: [
                gcnv.variant_id,
                gcnv.xpos,
            ],
        }[self]

    @property
    def genotype_entry_annotation_fns(self) -> list[Callable[..., hl.Expression]]:
        return {
            DatasetType.SNV: [
                snv.GQ,
                snv.AB,
                snv.DP,
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
        }[self]

    @property
    def sample_lookup_table_annotation_fns(self) -> list[Callable[..., hl.Expression]]:
        return {
            DatasetType.SNV: [
                sample_lookup_table.gt_stats,
            ],
            DatasetType.MITO: [
                sample_lookup_table.gt_stats,
            ],
            DatasetType.SV: [],
        }[self]
