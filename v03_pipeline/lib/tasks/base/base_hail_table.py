import hail as hl
import luigi

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget

logger = get_logger(__name__)


class BaseHailTableTask(luigi.Task):
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    dataset_type = luigi.EnumParameter(enum=DatasetType)

    def output(self) -> luigi.Target:
        raise NotImplementedError

    def complete(self) -> bool:
        logger.info(f'BaseHailTableTask: checking if {self.output().path} exists')
        return GCSorLocalFolderTarget(self.output().path).exists()

    def init_hail(self):
        # Need to use the GCP bucket as temp storage for very large callset joins
        hl.init(tmp_dir=Env.HAIL_TMPDIR, idempotent=True)

        # Interval ref data join causes shuffle death, this prevents it
        hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')  # noqa: SLF001


# NB: these are defined over luigi.Task instead of the BaseHailTableTask so that
# they work on file dependencies.


@luigi.Task.event_handler(luigi.Event.DEPENDENCY_DISCOVERED)
def dependency_discovered(task, dependency):
    logger.info(f'{task} dependency_discovered {dependency} at {task.output()}')


@luigi.Task.event_handler(luigi.Event.DEPENDENCY_MISSING)
def dependency_missing(task):
    logger.info(f'{task} dependency_missing at {task.output()}')


@luigi.Task.event_handler(luigi.Event.DEPENDENCY_PRESENT)
def dependency_present(task):
    logger.info(f'{task} dependency_present at {task.output()}')


@luigi.Task.event_handler(luigi.Event.START)
def start(task):
    logger.info(f'{task} start')


@luigi.Task.event_handler(luigi.Event.FAILURE)
def failure(task, _):
    logger.exception(f'{task} failure')


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def success(task):
    logger.info(f'{task} success')
