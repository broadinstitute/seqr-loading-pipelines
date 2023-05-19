import hail as hl
import luigi

from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome


class BasePipelineTask(luigi.Task):
    env = luigi.EnumParameter(enum=Env, default=Env.LOCAL)
    reference_genome = luigi.EnumParameter(
        enum=ReferenceGenome,
        default=ReferenceGenome.GRCh38,
    )
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    hail_temp_dir = luigi.OptionalParameter(
        default=None,
        description='Networked temporary directory used by hail for temporary file storage. Must be a network-visible file path.',
    )

    def run(self) -> None:
        if self.hail_temp_dir:
            # Need to use the GCP bucket as temp storage for very large callset joins
            hl.init(tmp_dir=self.hail_temp_dir)

        # Interval ref data join causes shuffle death, this prevents it
        hl._set_flags(use_new_shuffle='1')  # noqa: SLF001
