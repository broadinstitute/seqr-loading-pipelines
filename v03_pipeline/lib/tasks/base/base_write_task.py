import hail as hl
import luigi

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome


class BaseWriteTask(luigi.Task):
    env = luigi.EnumParameter(enum=Env)
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    hail_temp_dir = luigi.OptionalParameter(
        default=None,
        description='Networked temporary directory used by hail for temporary file storage. Must be a network-visible file path.',
    )
    single_partition = False

    def output(self) -> luigi.Target:
        raise NotImplementedError

    def complete(self) -> bool:
        raise NotImplementedError

    def init_hail(self):
        if self.hail_temp_dir:
            # Need to use the GCP bucket as temp storage for very large callset joins
            hl.init(tmp_dir=self.hail_temp_dir, idempotent=True)

        # Interval ref data join causes shuffle death, this prevents it
        hl._set_flags(use_new_shuffle='1')  # noqa: SLF001

    def run(self) -> None:
        self.init_hail()
        ht = self.create_ht()
        write(self.env, ht, self.output().path, checkpoint=False, single_partition=self.single_partition)

    def create_ht(self) -> hl.Table:
        raise NotImplementedError
