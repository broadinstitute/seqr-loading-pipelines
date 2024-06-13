import luigi

from v03_pipeline.lib.model import SampleType


class BaseLoadingRunParams(luigi.Task):
    # NB:
    # These params are "inherited" with the special
    # luigi.util.inherits function, copying params
    # but nothing else.
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    # HINT: OptionalParameter vs Parameter is significant here.
    # The default Parameter will case `None` to the string "None".
    imputed_sex_path = luigi.OptionalParameter(
        default=None,
        description='Optional path to a tsv of imputed sex values from the DRAGEN GVS pipeline.',
    )
    filters_path = luigi.OptionalParameter(
        default=None,
        description='Optional path to part two outputs from callset (VCF shards containing filter information)',
    )
    ignore_missing_samples_when_remapping = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
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
    is_new_gcnv_joint_call = luigi.BoolParameter(
        default=False,
        description='Is this a fully joint-called callset.',
    )
    liftover_ref_path = luigi.OptionalParameter(
        default='gs://hail-common/references/grch38_to_grch37.over.chain.gz',
        description='Path to GRCh38 to GRCh37 coordinates file',
    )
