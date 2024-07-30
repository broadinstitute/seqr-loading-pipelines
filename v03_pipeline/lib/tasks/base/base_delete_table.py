import hailtop.fs as hfs

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.tasks.base.task import BaseTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget

logger = get_logger(__name__)


class BaseDeleteTableTask(BaseTask):
    def complete(self) -> bool:
        logger.info(f'DeleteTableTask: checking if {self.output().path} exists')
        return (
            not GCSorLocalTarget(self.output().path).exists()
            and not GCSorLocalFolderTarget(self.output().path).exists()
        )

    def run(self) -> None:
        hfs.rmtree(self.output().path)
