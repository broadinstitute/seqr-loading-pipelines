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
    def base_reference_dataset_collection(self) -> ReferenceDatasetCollection | None:
        return {
            DatasetType.MITO: ReferenceDatasetCollection.COMBINED_MITO,
            DatasetType.SNV: ReferenceDatasetCollection.COMBINED,
        }.get(self)

    def annotatable_reference_dataset_collections(
        self,
        env: Env,
    ) -> set[ReferenceDatasetCollection]:
        rdcs = {
            DatasetType.SNV: {
                ReferenceDatasetCollection.HGMD,
                ReferenceDatasetCollection.INTERVAL,
            },
        }.get(self, set())
        if env == Env.LOCAL:
            return {rdc for rdc in rdcs if rdc.access_control == AccessControl.PUBLIC}
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
    def genotype_entries_type(
        self,
    ) -> hl.dtype:
        return {
            DatasetType.SNV: hl.tstruct(gq=hl.tint32, ab=hl.tfloat64, dp=hl.tint32),
        }[self]

    @property
    def veppable(self) -> bool:
        return self == DatasetType.SNV or self == DatasetType.MITO
