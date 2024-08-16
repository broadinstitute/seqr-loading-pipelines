import hailtop.fs as hfs
import luigi
import luigi.util

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingPipelineParams)
class BaseDeleteTableTask(luigi.Task):
    sample_type = luigi.EnumParameter(enum=SampleType)

    def complete(self) -> bool:
        logger.info(f'DeleteTableTask: checking if {self.output().path} exists')
        return (
            not GCSorLocalTarget(self.output().path).exists()
            and not GCSorLocalFolderTarget(self.output().path).exists()
        )

    def run(self) -> None:
        hfs.rmtree(self.output().path)
