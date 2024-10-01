import luigi
import luigi.util
import requests

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env
from v03_pipeline.lib.tasks import UpdateVariantAnnotationsTableWithNewSamplesTask
from v03_pipeline.lib.tasks.base.base_project_info_params import (
    BaseLoadingRunWithProjectInfoParams,
)

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingRunWithProjectInfoParams)
class TriggerHailBackendReload(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return self.clone(UpdateVariantAnnotationsTableWithNewSamplesTask)

    def run(self):
        url = f'{Env.HAIL_BACKEND_SERVICE_HOSTNAME}:{Env.HAIL_BACKEND_SERVICE_PORT}/reload_globals'
        res = requests.post(url, headers={'From': 'pipeline-runner'}, timeout=300)
        res.raise_for_status()
        self.done = True

    def complete(self):
        return self.done
