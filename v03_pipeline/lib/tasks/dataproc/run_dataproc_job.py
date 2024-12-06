import time

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
from v03_pipeline.lib.tasks.dataproc.misc import get_cluster_name

SEQR_PIPELINE_RUNNER_BUILD = f'gs://seqr-pipeline-runner-builds/{Env.DEPLOYMENT_TYPE}/{Env.PIPELINE_RUNNER_APP_VERSION}'
SUCCESS_STATE = 'DONE'

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingPipelineParams)
class RunDataprocJobTask(luigi.Task):
    run_id = luigi.Parameter()
    job_id = luigi.Parameter()
    additional_args = luigi.ListParameter(default=[])

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
                    'job_id': f'{self.job_id}-{self.run_id}',
                },
            )
        except Exception:  # noqa: BLE001
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
                        'job_id': f'{self.job_id}-{self.run_id}',
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
                            self.job_id,
                            '--local-scheduler',
                            '--reference-genome',
                            self.reference_genome.value,
                            '--dataset-type',
                            self.dataset_type.value,
                            *self.additional_args,
                        ],
                        'python_file_uris': f'{SEQR_PIPELINE_RUNNER_BUILD}/pyscripts.zip',
                    },
                },
            },
        )
        while True:
            if operation.done():
                result = operation.result()  # Will throw on failure!
                msg = f'Finished job {self.job_id}-{self.run_id}'
                logger.info(msg)
                break
            logger.info(f'Waiting for job completion {self.job_id}-{self.run_id}')
            time.sleep(3)
