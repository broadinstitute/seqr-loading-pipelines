import json
from typing import Literal

import hailtop.fs as hfs
from pydantic import AliasChoices, BaseModel, Field, field_validator, root_validator

from v03_pipeline.lib.misc.validation import SKIPPABLE_VALIDATIONS
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType

ALL_VALIDATIONS = 'all'
STRINGIFIED_SKIPPABLE_VALIDATIONS = {f.__name__ for f in SKIPPABLE_VALIDATIONS}
VALID_FILE_TYPES = ['vcf', 'vcf.gz', 'vcf.bgz', 'mt']


class LoadingPipelineRequest(BaseModel):
    callset_path: str
    project_guids: list[str] = Field(
        min_length=1,
        frozen=True,
        validation_alias=AliasChoices('project_guids', 'projects_to_run'),
    )
    sample_type: SampleType
    reference_genome: ReferenceGenome
    dataset_type: DatasetType
    skip_check_sex_and_relatedness: bool = False
    skip_expect_tdr_metrics: bool = False

    # New-style list
    validations_to_skip: list[Literal[*STRINGIFIED_SKIPPABLE_VALIDATIONS]] = []
    # Old-style boolean for backwards compatibility
    skip_validation: bool = Field(False, alias='skip_validation')

    @field_validator('validations_to_skip')
    @classmethod
    def must_be_known_validator(cls, validations_to_skip):
        for v in validations_to_skip:
            if v not in STRINGIFIED_SKIPPABLE_VALIDATIONS:
                msg = f'{v} is not a valid validator'
                raise ValueError(msg)
        return validations_to_skip

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

    @root_validator(
        pre=True,
    )  # the root validator runs before Pydantic parses or coerces field values.
    @classmethod
    def backwards_compatible_skip_validation(cls, values):
        if values.get('skip_validation'):
            values['validations_to_skip'] = STRINGIFIED_SKIPPABLE_VALIDATIONS
        if values.get('validations_to_skip') == ['all']:
            values['validations_to_skip'] = STRINGIFIED_SKIPPABLE_VALIDATIONS
        return values

    def json(self, *args, **kwargs):
        return json.dumps(self.dict(*args, **kwargs), *args, **kwargs)
