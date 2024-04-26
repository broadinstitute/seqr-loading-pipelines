import hailtop.fs as hfs

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.tasks.base.base_hail_table import BaseHailTableTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget

logger = get_logger(__name__)


class BaseDeleteTableTask(BaseHailTableTask):
    def complete(self) -> bool:
        logger.info(f'DeleteTableTask: checking if {self.output().path} exists')
        return (
            not GCSorLocalTarget(self.output().path).exists()
            and not GCSorLocalFolderTarget(self.output().path).exists()
        )

    def run(self) -> None:
        hfs.rmtree(self.output().path)
