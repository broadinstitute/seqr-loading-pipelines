import luigi

from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)


@luigi.util.inherits(BaseLoadingPipelineParams)
class BaseLoadingRunParams(luigi.Task):
    # The difference between the "Loading Run" params
    # and the "Loading Pipeline" params:
    # - These params are used during standard "runs"
    # of the pipeline that add a callset to the backend
    # data store.
    # - The "Loading Pipeline" params are shared with
    # tasks that may remove data from or change the
    # structure of the persisted Hail Tables.
    run_id = luigi.Parameter()
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    project_guids = luigi.ListParameter(default=[])
    project_pedigree_paths = luigi.ListParameter(default=[])
    skip_check_sex_and_relatedness = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    skip_expect_tdr_metrics = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    skip_validation = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    is_new_gcnv_joint_call = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
        description='Is this a fully joint-called callset.',
    )
