import hailtop.fs as hfs
from pydantic import AliasChoices, BaseModel, Field, conint, field_validator

from v03_pipeline.lib.core import DatasetType, ReferenceGenome, SampleType

MAX_LOADING_PIPELINE_ATTEMPTS = 3
VALID_FILE_TYPES = ['vcf', 'vcf.gz', 'vcf.bgz', 'mt']


class PipelineRunnerRequest(BaseModel, frozen=True):
    request_type: str

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.model_fields['request_type'].default = cls.__name__


class LoadingPipelineRequest(PipelineRunnerRequest):
    attempt_id: conint(ge=0, le=MAX_LOADING_PIPELINE_ATTEMPTS - 1) = 0
    callset_path: str
    project_guids: list[str] = Field(
        min_length=1,
        frozen=True,
        validation_alias=AliasChoices('project_guids', 'projects_to_run'),
    )
    sample_type: SampleType
    reference_genome: ReferenceGenome
    dataset_type: DatasetType
    skip_validation: bool = False
    skip_check_sex_and_relatedness: bool = False
    skip_expect_tdr_metrics: bool = False

    def incr_attempt(self):
        if self.attempt_id + 1 >= MAX_LOADING_PIPELINE_ATTEMPTS:
            return False
        self.attempt_id += 1
        return True

    @field_validator('callset_path')
    @classmethod
    def check_valid_callset_path(cls, callset_path: str) -> str:
        if not any(callset_path.endswith(file_type) for file_type in VALID_FILE_TYPES):
            msg = 'callset_path must be a VCF or a Hail Matrix Table'
            raise ValueError(msg)
        if '*' in callset_path and not hfs.ls(
            callset_path,
        ):  # note that hfs.ls throws an exception if it cannot find a non-wildcard path
            msg = 'callset_path must point to a shard pattern that exists'
            raise ValueError(msg)
        if '*' not in callset_path and not hfs.exists(callset_path):
            msg = 'callset_path must point to a file that exists'
            raise ValueError(msg)
        return callset_path


class DeleteFamiliesRequest(PipelineRunnerRequest):
    project_guid: str
    family_guids: list[str] = Field(
        min_length=1,
        frozen=True,
    )


class RebuildGtStatsRequest(PipelineRunnerRequest):
    project_guids: list[str] = Field(
        min_length=1,
        frozen=True,
    )
