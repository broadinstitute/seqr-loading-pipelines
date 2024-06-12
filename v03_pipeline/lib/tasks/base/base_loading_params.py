import luigi

from v03_pipeline.lib.model import SampleType


class BaseLoadingParams(luigi.Task):
    # NB:
    # These params are "inherited" with the special
    # luigi.util.inherits function, copying params
    # but nothing else.
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    imputed_sex_path = luigi.Parameter(
        default=None,
        description='Optional path to a tsv of imputed sex values from the DRAGEN GVS pipeline.',
    )
    filters_path = luigi.Parameter(
        default=None,
        description='Optional path to part two outputs from callset (VCF shards containing filter information)',
    )
    validate = luigi.BoolParameter(
        default=True,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    force = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    check_sex_and_relatedness = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
