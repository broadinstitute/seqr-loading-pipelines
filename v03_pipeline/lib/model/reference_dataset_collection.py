from __future__ import annotations

from enum import Enum

import hail as hl

from v03_pipeline.lib.model.dataset_type import DatasetType
from v03_pipeline.lib.model.definitions import AccessControl, ReferenceGenome


class ReferenceDatasetCollection(Enum):
    COMBINED = 'combined'
    HGMD = 'hgmd'
    INTERVAL = 'interval'

    @property
    def access_control(self) -> AccessControl:
        if self == ReferenceDatasetCollection.HGMD:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    @property
    def requires_annotation(self) -> bool:
        return self == ReferenceDatasetCollection.INTERVAL

    def datasets(self, dataset_type: DatasetType) -> list[str]:
        return {
            (ReferenceDatasetCollection.COMBINED, DatasetType.SNV_INDEL): [
                'cadd',
                'clinvar',
                'dbnsfp',
                'eigen',
                'exac',
                'gnomad_exomes',
                'gnomad_genomes',
                'mpc',
                'primate_ai',
                'splice_ai',
                'topmed',
            ],
            (ReferenceDatasetCollection.COMBINED, DatasetType.MITO): [
                'clinvar_mito',
                'dbnsfp_mito',
                'gnomad_mito',
                'helix_mito',
                'hmtvar',
                'mitomap',
                'mitimpact',
            ],
            (ReferenceDatasetCollection.HGMD, DatasetType.SNV_INDEL): ['hgmd'],
            (ReferenceDatasetCollection.INTERVAL, DatasetType.SNV_INDEL): [
                'gnomad_non_coding_constraint',
                'screen',
            ],
            (ReferenceDatasetCollection.INTERVAL, DatasetType.MITO): [
                'high_constraint_region_mito',
            ],
        }[(self, dataset_type)]

    def table_key_type(
        self,
        reference_genome: ReferenceGenome,
    ) -> hl.tstruct:
        default_key = hl.tstruct(
            locus=hl.tlocus(reference_genome.value),
            alleles=hl.tarray(hl.tstr),
        )
        return {
            ReferenceDatasetCollection.INTERVAL: hl.tstruct(
                interval=hl.tinterval(hl.tlocus(reference_genome.value)),
            ),
        }.get(self, default_key)

    @classmethod
    def for_dataset_type(
        cls, dataset_type: DatasetType,
    ) -> list[ReferenceDatasetCollection]:
        return {
            DatasetType.SNV_INDEL: [
                ReferenceDatasetCollection.COMBINED,
                ReferenceDatasetCollection.INTERVAL,
                ReferenceDatasetCollection.HGMD,
            ],
            DatasetType.MITO: [
                ReferenceDatasetCollection.COMBINED,
                ReferenceDatasetCollection.INTERVAL,
            ],
        }[self]
