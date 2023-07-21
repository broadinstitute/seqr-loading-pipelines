from __future__ import annotations

from enum import Enum

import hail as hl

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
        }.get(self, set())

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
        }.get(self, set())
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
            DatasetType.MITO: ['GT', 'DP', 'MQ', 'HL', 'mito_cn', 'contamination'],
        }.get(self, [])

    @property
    def row_fields(
        self,
    ) -> list[str]:
        return {
            DatasetType.SNV: ['rsid', 'filters'],
            DatasetType.MITO: ['rsid', 'filters'],
        }[self]

    @property
    def veppable(self) -> bool:
        return self == DatasetType.SNV or self == DatasetType.MITO
