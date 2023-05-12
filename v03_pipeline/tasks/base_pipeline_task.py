import luigi

from v03_pipeline.core.definitions import (
    DatasetType,
    Env,
    ReferenceGenome,
    SampleSource,
    SampleType,
)


class BasePipelineTask(luigi.Task):
    class InvalidParameterError(Exception):
        pass

    env = luigi.EnumParameter(enum=Env, default=Env.LOCAL)
    reference_genome = luigi.EnumParameter(
        enum=ReferenceGenome,
        default=ReferenceGenome.GRCh38,
    )
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    sample_type = luigi.EnumParameter(enum=SampleType)
    sample_source = luigi.EnumParameter(enum=SampleSource, default=SampleSource.LOCAL)

    def validate(self) -> None:
        if self.sample_type not in self.dataset_type.sample_types:
            msg = f'{self.dataset_type.value} data does not support {self.sample_type.value} samples'
            raise self.InvalidParameterError(msg)

        if (self.env == Env.LOCAL and self.sample_source != SampleSource.LOCAL) or (
            self.sample_source == SampleSource.LOCAL and self.env != Env.LOCAL
        ):
            msg = 'Only local samples are allowed when running the pipeline locally'
            raise self.InvalidParameterError(msg)

    def run(self) -> None:
        self.validate()
