import luigi
import luigi.util
import requests

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.write_success_file import WriteSuccessFileTask

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingRunParams)
class TriggerHailBackendReload(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return self.clone(WriteSuccessFileTask)

    def run(self):
        url = f'http://{Env.HAIL_BACKEND_SERVICE_HOSTNAME}:{Env.HAIL_BACKEND_SERVICE_PORT}/reload_globals'
        res = requests.post(url, headers={'From': 'pipeline-runner'}, timeout=300)
        res.raise_for_status()
        self.done = True

    def complete(self):
        return self.done
