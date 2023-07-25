from __future__ import annotations

from enum import Enum
from typing import Callable

import hail as hl

from v03_pipeline.lib.annotations import gcnv, sample_lookup_table, shared, snv, sv
from v03_pipeline.lib.model.definitions import AccessControl, Env, ReferenceGenome
from v03_pipeline.lib.model.reference_dataset_collection import (
    ReferenceDatasetCollection,
)


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
    def entries_fields(
        self,
    ) -> list[str]:
        return {
            DatasetType.SNV: ['GT', 'AD', 'GQ'],
        }[self]

    @property
    def row_fields(
        self,
    ) -> list[str]:
        return {
            DatasetType.SNV: ['rsid', 'filters'],
        }[self]

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
                shared.rg37_locus,
                shared.sorted_transcript_consequences,
                shared.variant_id,
                shared.xpos,
            ],
            DatasetType.SV: [
                shared.rg37_locus,
                sv.variant_id,
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
                shared.GT,
            ],
        }[self]

    @property
    def sample_lookup_table_annotation_fns(self) -> list[Callable[..., hl.Expression]]:
        return {
            DatasetType.SNV: [
                sample_lookup_table.gt_stats,
            ],
        }[self]
