import time

import google.api_core.exceptions
import google.cloud.dataproc_v1.types.jobs
import luigi
from google.cloud import dataproc_v1 as dataproc

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.core import Env
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.dataproc.create_dataproc_cluster import (
    CreateDataprocClusterTask,
)
from v03_pipeline.lib.tasks.dataproc.misc import get_cluster_name, to_kebab_str_args

SEQR_PIPELINE_RUNNER_BUILD = f'gs://seqr-pipeline-runner-builds/{Env.DEPLOYMENT_TYPE}/{Env.PIPELINE_RUNNER_APP_VERSION}'
TIMEOUT_S = 172800  # 2 days

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingPipelineParams)
class BaseRunJobOnDataprocTask(luigi.Task):
    attempt_id = luigi.IntParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = dataproc.JobControllerClient(
            client_options={
                'api_endpoint': f'{Env.GCLOUD_REGION}-dataproc.googleapis.com:443',
            },
        )

    @property
    def task(self):
        raise NotImplementedError

    @property
    def job_id(self):
        return f'{self.task.task_family}-{self.run_id}-{self.attempt_id}'

    def requires(self) -> [luigi.Task]:
        return [self.clone(CreateDataprocClusterTask)]

    def complete(self) -> bool:
        try:
            job = self.client.get_job(
                request={
                    'project_id': Env.GCLOUD_PROJECT,
                    'region': Env.GCLOUD_REGION,
                    'job_id': self.job_id,
                },
            )
        except google.api_core.exceptions.NotFound:
            return False
        if job.status.state in {
            google.cloud.dataproc_v1.types.jobs.JobStatus.State.CANCELLED,
            google.cloud.dataproc_v1.types.jobs.JobStatus.State.ERROR,
            google.cloud.dataproc_v1.types.jobs.JobStatus.State.ATTEMPT_FAILURE,
        }:
            msg = f'Job {self.job_id} entered {job.status.state.name} state'
            logger.error(msg)
            logger.error(job.status.details)
        return (
            job.status.state == google.cloud.dataproc_v1.types.jobs.JobStatus.State.DONE
        )

    def run(self):
        operation = self.client.submit_job_as_operation(
            request={
                'project_id': Env.GCLOUD_PROJECT,
                'region': Env.GCLOUD_REGION,
                'job': {
                    'reference': {
                        'job_id': self.job_id,
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
                            self.task.task_family,
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
        wait_s = 0
        while wait_s < TIMEOUT_S:
            if operation.done():
                operation.result()  # Will throw on failure!
                msg = f'Finished {self.job_id}'
                logger.info(msg)
                break
            logger.info(
                f'Waiting for job completion {self.job_id}',
            )
            time.sleep(3)
            wait_s += 3
