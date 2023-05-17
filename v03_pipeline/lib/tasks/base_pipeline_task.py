import luigi

from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome, SampleType


class BasePipelineTask(luigi.Task):
    env = luigi.EnumParameter(enum=Env, default=Env.LOCAL)
    reference_genome = luigi.EnumParameter(
        enum=ReferenceGenome,
        default=ReferenceGenome.GRCh38,
    )
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    sample_type = luigi.EnumParameter(enum=SampleType)
