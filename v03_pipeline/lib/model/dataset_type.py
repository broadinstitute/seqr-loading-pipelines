from __future__ import annotations

from enum import Enum

import hail as hl

from v03_pipeline.lib.model.definitions import (
    AccessControl,
    Env,
    ReferenceGenome,
    SampleFileType,
)
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

    def selectable_reference_dataset_collections(
        self,
        env: Env,
    ) -> list[ReferenceDatasetCollection]:
        rdcs = {
            DatasetType.SNV: [
                ReferenceDatasetCollection.HGMD,
                ReferenceDatasetCollection.INTERVAL_REFERENCE,
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
    def sample_file_type(self) -> SampleFileType:
        if self == DatasetType.GCNV:
            return SampleFileType.BED
        return SampleFileType.VCF

    @property
    def veppable(self) -> bool:
        return self == DatasetType.SNV or self == DatasetType.MITO
