import hail as hl

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.tasks.base.base_task import BaseTask


class BaseUpdateTask(BaseTask):
    def run(self) -> None:
        self.init_hail()
        if not self.output().exists():
            ht = self.initialize_table()
        else:
            ht = hl.read_table(self.output().path)
        ht = self.update_table(ht)
        write(ht, self.output().path)

    def initialize_table(self) -> hl.Table:
        raise NotImplementedError

    def update_table(self, ht: hl.Table) -> hl.Table:
        raise NotImplementedError
