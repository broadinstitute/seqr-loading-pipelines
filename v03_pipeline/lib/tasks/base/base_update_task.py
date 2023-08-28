import hail as hl
import luigi

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import DataRoot, DatasetType, ReferenceGenome


class BaseUpdateTask(luigi.Task):
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    dataset_type = luigi.EnumParameter(enum=DatasetType)
    n_partitions = None

    def output(self) -> luigi.Target:
        raise NotImplementedError

    def complete(self) -> bool:
        raise NotImplementedError

    def init_hail(self):
        # Need to use the GCP bucket as temp storage for very large callset joins
        hl.init(tmp_dir=DataRoot.HAIL_TMPDIR, idempotent=True)

        # Interval ref data join causes shuffle death, this prevents it
        hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')  # noqa: SLF001

    def run(self) -> None:
        self.init_hail()
        if not self.output().exists():
            ht = self.initialize_table()
        else:
            ht = hl.read_table(self.output().path)
        ht = self.update_table(ht)
        write(
            ht,
            self.output().path,
            checkpoint=True,
            n_partitions=self.n_partitions,
        )

    def initialize_table(self) -> hl.Table:
        raise NotImplementedError

    def update_table(self, ht: hl.Table) -> hl.Table:
        raise NotImplementedError