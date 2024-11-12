from enum import Enum

from v03_pipeline.lib.model import AccessControl, DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.reference_data.config import CONFIG


class ReferenceDataset(str, Enum):
    cadd = 'cadd'
    hgmd = 'hgmd'

    @property
    def _config(self) -> dict:
        return CONFIG[self.value]

    @classmethod
    def for_reference_genome_dataset_type(
        cls,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
    ) -> list['ReferenceDataset']:
        reference_datasets = {
            (ReferenceGenome.GRCh37, DatasetType.SNV_INDEL): [
                ReferenceDataset.cadd,
                ReferenceDataset.hgmd,
            ],
            (ReferenceGenome.GRCh38, DatasetType.SNV_INDEL): [
                ReferenceDataset.cadd,
                ReferenceDataset.hgmd,
            ],
        }.get((reference_genome, dataset_type), [])
        if not Env.ACCESS_PRIVATE_REFERENCE_DATASETS:
            return [
                rd
                for rd in reference_datasets
                if rd.access_control == AccessControl.PUBLIC
            ]
        return reference_datasets

    @property
    def access_control(self) -> AccessControl:
        if self == ReferenceDataset.hgmd:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    def version(self, reference_genome: ReferenceGenome) -> str:
        return self._config[reference_genome]['version']

    def raw_dataset_path(self, reference_genome: ReferenceGenome) -> str | list[str]:
        return self._config[reference_genome]['raw_dataset_path']

    def load_parsed_dataset_func(self) -> callable:
        return self._config['load_parsed_dataset_func']
