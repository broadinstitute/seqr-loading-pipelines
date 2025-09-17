import hailtop.fs as hfs
from pydantic import BaseModel, Field, field_validator

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType

VALID_FILE_TYPES = ['vcf', 'vcf.gz', 'vcf.bgz', 'mt']


class LoadingPipelineRequest(BaseModel):
    callset_path: str
    projects_to_run: list[str] = Field(min_length=0, frozen=True, default_factory=list)
    project_guids: list[str] = Field(min_length=0, frozen=True, default_factory=list)
    sample_type: SampleType
    reference_genome: ReferenceGenome
    dataset_type: DatasetType
    skip_validation: bool = False
    skip_check_sex_and_relatedness: bool = False
    skip_expect_tdr_metrics: bool = False

    @field_validator('callset_path')
    @classmethod
    def check_valid_callset_path(cls, callset_path: str) -> str:
        if not any(callset_path.endswith(file_type) for file_type in VALID_FILE_TYPES):
            msg = 'callset_path must be a VCF or a Hail Matrix Table'
            raise ValueError(msg)
        if not hfs.exists(callset_path):
            msg = 'callset_path must point to a file that exists'
            raise ValueError(msg)
        return callset_path

    def __str__(self) -> str:
        return '\n'.join(
            [
                f'Callset Path: {self.callset_path}',
                f'Projects Guids: {",".join(self.projects_to_run) if self.projects_to_run else ",".join(self.project_guids)}',
                f'Reference Genome: {self.reference_genome.value}',
                f'Dataset Type: {self.dataset_type.value}',
                f'Sample Type: {self.sample_type.value}',
                f'Skip Validation: {self.skip_validation}',
                f'Skip Sex & Relatedness: {self.skip_check_sex_and_relatedness}',
                f'Skip Expect TDR Metrics: {self.skip_expect_tdr_metrics}',
            ],
        )
