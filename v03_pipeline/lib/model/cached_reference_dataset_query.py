from collections.abc import Callable
from enum import Enum

import hail as hl

from v03_pipeline.lib.model.dataset_type import DatasetType
from v03_pipeline.lib.model.definitions import AccessControl, ReferenceGenome
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.reference_data.queries import (
    clinvar_path_variants,
    gnomad_coding_and_noncoding_variants,
    gnomad_qc,
    high_af_variants,
)


class CachedReferenceDatasetQuery(Enum):
    CLINVAR_PATH_VARIANTS = 'clinvar_path_variants'
    GNOMAD_CODING_AND_NONCODING_VARIANTS = 'gnomad_coding_and_noncoding_variants'
    GNOMAD_QC = 'gnomad_qc'
    HIGH_AF_VARIANTS = 'high_af_variants'

    @property
    def access_control(self) -> AccessControl:
        if self == CachedReferenceDatasetQuery.GNOMAD_QC:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    @property
    def dataset(self) -> str | None:
        return {
            CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS: 'clinvar',
            CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS: 'gnomad_genomes',
            CachedReferenceDatasetQuery.GNOMAD_QC: 'gnomad_qc',
            CachedReferenceDatasetQuery.HIGH_AF_VARIANTS: 'gnomad_genomes',
        }.get(self)

    @property
    def query_raw_dataset(self) -> bool:
        return {
            CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS: True,
            CachedReferenceDatasetQuery.GNOMAD_QC: True,
        }.get(self, false)

    @property
    def query(self) -> Callable[[hl.Table, ReferenceGenome], hl.Table]:
        return {
            CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS: clinvar_path_variants,
            CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS: gnomad_coding_and_noncoding_variants,
            CachedReferenceDatasetQuery.GNOMAD_QC: gnomad_qc,
            CachedReferenceDatasetQuery.HIGH_AF_VARIANTS: high_af_variants,
        }[self]

    @classmethod
    def for_reference_genome_dataset_type(
        cls,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
    ) -> list['CachedReferenceDatasetQuery']:
        crdqs = {
            (ReferenceGenome.GRCh38, DatasetType.SNV_INDEL): list(cls),
            (ReferenceGenome.GRCh38, DatasetType.MITO): [
                CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS,
            ],
            (ReferenceGenome.GRCh37, DatasetType.SNV_INDEL): list(cls),
        }.get((reference_genome, dataset_type), [])
        if not Env.ACCESS_PRIVATE_REFERENCE_DATASETS:
            return [
                crdq for crdq in crdqs if crdq.access_control == AccessControl.PUBLIC
            ]
        return crdqs
