import hail as hl

from v03_pipeline.lib.annotations.liftover import remove_liftover
from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.tasks.base.base_hail_table import BaseHailTableTask


class BaseWriteTask(BaseHailTableTask):
    def run(self) -> None:
        self.init_hail()
        ht = self.create_table()
        write(ht, self.output().path)
        # Set force to false after run, allowing "complete()" to succeeded
        # when dependencies are re-evaluated.
        self.force = False
        # Ensure any cached liftover files within Hail are cleared
        remove_liftover()

    def create_table(self) -> hl.Table:
        raise NotImplementedError
