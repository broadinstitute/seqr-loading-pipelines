import luigi

from v03_pipeline.lib.model import SampleType


class BaseLoadingRunParams(luigi.Task):
    # NB:
    # These params are "inherited" with the special
    # luigi.util.inherits function, copying params
    # but nothing else.
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    ignore_missing_samples_when_remapping = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    force = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    skip_check_sex_and_relatedness = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    skip_expect_filters = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    skip_validation = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    is_new_gcnv_joint_call = luigi.BoolParameter(
        default=False,
        description='Is this a fully joint-called callset.',
    )
