import luigi

from v03_pipeline.lib.model import DatasetType, ReferenceGenome


class BaseLoadingPipelineParams(luigi.Task):
    # NB:
    # These params are "inherited" with the special
    # luigi.util.inherits function, copying params
    # but nothing else.
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    dataset_type = luigi.EnumParameter(enum=DatasetType)
