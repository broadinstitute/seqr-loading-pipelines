from enum import Enum

import hail as hl

from v03_pipeline.lib.model.dataset_type import DatasetType
from v03_pipeline.lib.model.definitions import AccessControl, ReferenceGenome
from v03_pipeline.lib.model.environment import Env


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
        }.get((self, dataset_type), [])

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
    def for_reference_genome_dataset_type(
        cls,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
    ) -> list['ReferenceDatasetCollection']:
        rdcs = {
            (ReferenceGenome.GRCh38, DatasetType.SNV_INDEL): [
                ReferenceDatasetCollection.COMBINED,
                ReferenceDatasetCollection.INTERVAL,
                ReferenceDatasetCollection.HGMD,
            ],
            (ReferenceGenome.GRCh38, DatasetType.MITO): [
                ReferenceDatasetCollection.COMBINED,
                ReferenceDatasetCollection.INTERVAL,
            ],
            (ReferenceGenome.GRCh37, DatasetType.SNV_INDEL): [
                ReferenceDatasetCollection.COMBINED,
                ReferenceDatasetCollection.HGMD,
            ],
        }.get((reference_genome, dataset_type), [])
        if not Env.ACCESS_PRIVATE_REFERENCE_DATASETS:
            return [rdc for rdc in rdcs if rdc.access_control == AccessControl.PUBLIC]
        return rdcs

    @classmethod
    def for_dataset(
        cls,
        dataset: str,
        dataset_type: DatasetType,
    ) -> 'ReferenceDatasetCollection':
        for rdc in cls:
            if dataset in rdc.datasets(dataset_type):
                return rdc

        err_msg = f'Dataset "{dataset}" not found in any reference dataset collection'
        raise ValueError(err_msg)
