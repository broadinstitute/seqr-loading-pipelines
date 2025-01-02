import time

import google.api_core.exceptions
import luigi
from google.cloud import dataproc_v1 as dataproc

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.dataproc.create_dataproc_cluster import (
    CreateDataprocClusterTask,
)
from v03_pipeline.lib.tasks.dataproc.misc import get_cluster_name, to_kebab_str_args

DONE_STATE = 'DONE'
ERROR_STATE = 'ERROR'
SEQR_PIPELINE_RUNNER_BUILD = f'gs://seqr-pipeline-runner-builds/{Env.DEPLOYMENT_TYPE}/{Env.PIPELINE_RUNNER_APP_VERSION}'


logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingPipelineParams)
class BaseRunJobOnDataprocTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = dataproc.JobControllerClient(
            client_options={
                'api_endpoint': f'{Env.GCLOUD_REGION}-dataproc.googleapis.com:443',
            },
        )

    def task_name(self):
        return self.get_task_family().split('.')[-1]

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
                    'job_id': f'{self.task_name}-{self.run_id}',
                },
            )
        except google.api_core.exceptions.NotFound:
            return False
        else:
            if job.status.state == ERROR_STATE:
                msg = f'Job {self.task_name}-{self.run_id} entered ERROR state'
                logger.error(msg)
                logger.error(job.status.details)
            return job.status.state == DONE_STATE

    def run(self):
        operation = self.client.submit_job_as_operation(
            request={
                'project_id': Env.GCLOUD_PROJECT,
                'region': Env.GCLOUD_REGION,
                'job': {
                    'reference': {
                        'job_id': f'{self.task_name}-{self.run_id}',
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
                            self.task_name,
                            '--local-scheduler',
                            *to_kebab_str_args(self),
                        ],
                        'python_file_uris': [
                            f'{SEQR_PIPELINE_RUNNER_BUILD}/pyscripts.zip',
                        ],
                    },
                },
            },
        )
        while True:
            if operation.done():
                operation.result()  # Will throw on failure!
                msg = f'Finished {self.task_name}-{self.run_id}'
                logger.info(msg)
                break
            logger.info(
                f'Waiting for job completion {self.task_name}-{self.run_id}',
            )
            time.sleep(3)
