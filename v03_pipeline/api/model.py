import hailtop.fs as hfs
from pydantic import BaseModel, Field, field_validator

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType

VALID_FILE_TYPES = ['vcf', 'vcf.gz', 'vcf.bgz', 'mt']


class LoadingPipelineRequest(BaseModel):
    callset_path: str
    projects_to_run: list[str] = Field(min_length=1, frozen=True)
    sample_type: SampleType
    reference_genome: ReferenceGenome
    dataset_type: DatasetType
    force: bool = False
    ignore_missing_samples_when_remapping: bool = False
    skip_validation: bool = False

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
