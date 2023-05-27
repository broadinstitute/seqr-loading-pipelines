from __future__ import annotations

from enum import Enum

from v03_pipeline.lib.model.definitions import AccessControl, DatasetType


class ReferenceDatasetCollection(Enum):
    COMBINED = 'combined'
    COMBINED_MITO = 'combined_mito'
    HGMD = 'hgmd'
    INTERVAL_REFERENCE = 'interval_reference'

    @property
    def access_control(self) -> AccessControl:
        if self == ReferenceDatasetCollection.HGMD:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    @property
    def dataset_type(self) -> DatasetType:
        return {
            ReferenceDatasetCollection.COMBINED: DatasetType.SNV,
            ReferenceDatasetCollection.COMBINED_MITO: DatasetType.MITO,
            ReferenceDatasetCollection.HGMD: DatasetType.SNV,
            ReferenceDatasetCollection.INTERVAL_REFERENCE: DatasetType.SNV,
        }[self]

    @property
    def reference_datasets(self) -> list[str]:
        return {
            ReferenceDatasetCollection.COMBINED: [
                'cadd',
                'clinvar',
                'dbnsfp',
                'eigen',
                'exac',
                'geno2mp',
                'gnomad_exome_coverage',
                'gnomad_exomes',
                'gnomad_genome_coverage',
                'gnomad_genomes',
                'mpc',
                'primate_ai',
                'splice_ai',
                'topmed',
            ],
            ReferenceDatasetCollection.COMBINED_MITO: [
                'clinvar',
                'dbnsfp_mito',
                'gnomad_mito',
                'helix_mito',
                'hmtvar',
                'mitomap',
                'mitimpact',
            ],
            ReferenceDatasetCollection.HGMD: ['hgmd'],
            ReferenceDatasetCollection.INTERVAL_REFERENCE: [
                'gnomad_non_coding_constraint',
                'screen',
            ],
        }[self]
