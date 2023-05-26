from __future__ import annotations

from enum import Enum

import hail as hl


class AccessControl(Enum):
    PUBLIC = 'public'
    PRIVATE = 'private'


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


class DataRoot(Enum):
    LOCAL_DATASETS = 'seqr-datasets'
    LOCAL_REFERENCE_DATA = 'seqr-reference-data'
    SEQR_DATASETS = 'gs://seqr-datasets'
    SEQR_LOADING_TEMP = 'gs://seqr-loading-temp'
    SEQR_REFERENCE_DATA = 'gs://seqr-reference-data'
    SEQR_REFERENCE_DATA_PRIVATE = 'gs://seqr-reference-data-private'
    SEQR_SCRATCH_TEMP = 'gs://seqr-scratch-temp'


class Env(Enum):
    DEV = 'dev'
    LOCAL = 'local'
    PROD = 'prod'
    TEST = 'test'


class PipelineVersion(Enum):
    V02 = 'v02'
    V03 = 'v03'


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

    @property
    def sample_file_type(self) -> SampleFileType:
        if self == DatasetType.GCNV:
            return SampleFileType.BED
        return SampleFileType.VCF


class ReferenceGenome(Enum):
    GRCh37 = 'GRCh37'
    GRCh38 = 'GRCh38'

    @property
    def v02_value(self):
        return self.value[-2:]


class SampleFileType(Enum):
    BED = 'BED'
    VCF = 'VCF'


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'
