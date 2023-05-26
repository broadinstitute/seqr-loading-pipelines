from __future__ import annotations

from enum import Enum

import hail as hl

from v03_pipeline.lib.annotations import (
    Annotation,
    hgmd,
    interval_reference,
    rg37_locus,
    vep,
)
from v03_pipeline.lib.definitions import (
    ReferenceDatasetCollection,
    ReferenceGenome,
    SampleFileType,
)


class DatasetType(Enum):
    GCNV = 'GCNV'
    MITO = 'MITO'
    SNV = 'SNV'
    SV = 'SV'

    @property
    def annotations(self) -> list[Annotation]:
        return {
            DatasetType.GCNV: [],
            DatasetType.MITO: [
                rg37_locus,
                vep,
            ],
            DatasetType.SV: [
                rg37_locus,
            ],
            DatasetType.SNV: [
                hgmd,
                interval_reference,
                rg37_locus,
                vep,
            ],
        }[self]

    @property
    def base_reference_dataset_collection(self) -> ReferenceDatasetCollection | None:
        return {
            DatasetType.MITO: ReferenceDatasetCollection.COMBINED_MITO,
            DatasetType.SNV: ReferenceDatasetCollection.COMBINED,
        }.get(self)

    @property
    def supplemental_reference_dataset_collections(
        self,
    ) -> list[ReferenceDatasetCollection]:
        return {
            DatasetType.SNV: [
                ReferenceDatasetCollection.HGMD,
                ReferenceDatasetCollection.INTERVAL_REFERENCE,
            ],
        }.get(self, [])

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
