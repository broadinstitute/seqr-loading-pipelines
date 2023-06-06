import hail as hl
import luigi

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome


class BasePipelineTask(luigi.Task):
    env = luigi.EnumParameter(enum=Env)
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    hail_temp_dir = luigi.OptionalParameter(
        default=None,
        description='Networked temporary directory used by hail for temporary file storage. Must be a network-visible file path.',
    )

    def output(self) -> luigi.Target:
        raise NotImplementedError

    def complete(self) -> bool:
        raise NotImplementedError

    def init_hail(self):
        if self.hail_temp_dir:
            # Need to use the GCP bucket as temp storage for very large callset joins
            # `idempotent` makes this a no-op if already called.
            hl.init(tmp_dir=self.hail_temp_dir, idempotent=True)

        # Interval ref data join causes shuffle death, this prevents it
        hl._set_flags(use_new_shuffle='1')  # noqa: SLF001

    def run(self) -> None:
        self.init_hail()
        if not self.output().exists():
            ht = self.initialize_table()
        else:
            ht = hl.read_table(self.output().path)
        ht = self.update(ht)
        write(self.env, ht, self.output().path)

    def initialize_table(self) -> hl.Table:
        raise NotImplementedError

    def update(self, ht: hl.Table) -> hl.Table:
        raise NotImplementedError
