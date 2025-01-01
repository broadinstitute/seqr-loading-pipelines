import re
import time

import google.api_core.exceptions
import luigi
from google.cloud import dataproc_v1 as dataproc

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env
from v03_pipeline.lib.paths import pipeline_run_success_file_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.dataproc.create_dataproc_cluster import (
    CreateDataprocClusterTask,
)
from v03_pipeline.lib.tasks.dataproc.misc import get_cluster_name
from v03_pipeline.lib.tasks.files import GCSorLocalTarget

SEQR_PIPELINE_RUNNER_BUILD = f'gs://seqr-pipeline-runner-builds/{Env.DEPLOYMENT_TYPE}/{Env.PIPELINE_RUNNER_APP_VERSION}'
SUCCESS_STATE = 'DONE'

logger = get_logger(__name__)


def snake_to_kebab_arg(snake_string: str) -> str:
    return '--' + re.sub(r'\_', '-', snake_string).lower()


@luigi.util.inherits(BaseLoadingRunParams)
class WriteSuccessFileOnDataprocTask(luigi.Task):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            pipeline_run_success_file_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = dataproc.JobControllerClient(
            client_options={
                'api_endpoint': f'{Env.GCLOUD_REGION}-dataproc.googleapis.com:443',
            },
        )

    def requires(self) -> [luigi.Task]:
        return [self.clone(CreateDataprocClusterTask)]

    def complete(self) -> bool:
        if not self.dataset_type.requires_dataproc:
            msg = f'{self.dataset_type} should not require a dataproc job'
            raise RuntimeError(msg)
        try:
            job = self.client.get_job(
                request={
                    'project_id': Env.GCLOUD_PROJECT,
                    'region': Env.GCLOUD_REGION,
                    'job_id': f'WriteSuccessFileTask-{self.run_id}',
                },
            )
        except google.api_core.exceptions.NotFound:
            return False
        else:
            return job.status.state == SUCCESS_STATE

    def run(self):
        operation = self.client.submit_job_as_operation(
            request={
                'project_id': Env.GCLOUD_PROJECT,
                'region': Env.GCLOUD_REGION,
                'job': {
                    'reference': {
                        'job_id': f'WriteSuccessFileTask-{self.run_id}',
                    },
                    'placement': {
                        'cluster_name': get_cluster_name(
                            self.reference_genome,
                            self.run_id,
                        ),
                    },
                    'pyspark_job': {
                        'main_python_file_uri': f'{SEQR_PIPELINE_RUNNER_BUILD}/bin/run_task.py',
                        'args': [
                            'WriteSuccessFileTask',
                            '--local-scheduler',
                            *[
                                e
                                for k, v in self.to_str_params().items()
                                for e in (snake_to_kebab_arg(k), v)
                            ],
                        ],
                        'python_file_uris': f'{SEQR_PIPELINE_RUNNER_BUILD}/pyscripts.zip',
                    },
                },
            },
        )
        while True:
            if operation.done():
                _ = operation.result()  # Will throw on failure!
                msg = f'Finished WriteSuccessFileTask-{self.run_id}'
                logger.info(msg)
                break
            logger.info(
                f'Waiting for job completion WriteSuccessFileTask-{self.run_id}',
            )
            time.sleep(3)
