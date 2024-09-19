import luigi
import requests

from luigi_pipeline.lib.hail_tasks import GCSorLocalTarget
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env
from v03_pipeline.lib.paths import hail_backend_reload_success_for_run_path
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingPipelineParams)
class TriggerHailBackendReload(luigi.Task):
    run_id = luigi.Parameter()

    def output(self):
        return GCSorLocalTarget(
            hail_backend_reload_success_for_run_path(
                reference_genome=self.reference_genome,
                dataset_type=self.dataset_type,
                run_id=self.run_id,
            ),
        )

    def complete(self):
        return GCSorLocalTarget(self.output().path).exists()

    def run(self):
        url = f'{Env.HAIL_BACKEND_SERVICE_HOSTNAME}:{Env.HAIL_BACKEND_SERVICE_PORT}/reload_globals'
        res = requests.post(url, headers={'From': 'loading-pipelines'}, timeout=300)
        res.raise_for_status()
        with self.output().open('w') as f:
            f.write('success!')
