import time

import google.api_core.exceptions
import hail as hl
import luigi
from google.cloud import dataproc_v1 as dataproc
from pip._internal.operations import freeze as pip_freeze

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env, ReferenceGenome
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)

CLUSTER_NAME_PREFIX = 'pipeline-runner'
DEBIAN_IMAGE = '2.1.33-debian11'
HAIL_VERSION = hl.version().split('-')[0]
INSTANCE_TYPE = 'n1-highmem-8'
PKGS = '|'.join(pip_freeze.freeze())
SUCCESS_STATE = 'RUNNING'

logger = get_logger(__name__)


def get_cluster_config(reference_genome: ReferenceGenome, run_id: str):
    return {
        'project_id': Env.GCLOUD_PROJECT,
        'cluster_name': f'{CLUSTER_NAME_PREFIX}-{reference_genome.value.lower()}-{run_id}',
        'config': {
            'gce_cluster_config': {
                'zone_uri': Env.GCLOUD_ZONE,
                'metadata': {
                    'WHEEL': f'gs://hail-common/hailctl/dataproc/{HAIL_VERSION}/hail-{HAIL_VERSION}-py3-none-any.whl',
                    'PKGS': PKGS,
                    'DEPLOYMENT_TYPE': Env.DEPLOYMENT_TYPE,
                    'REFERENCE_GENOME': reference_genome.value,
                    'PIPELINE_RUNNER_APP_VERSION': Env.PIPELINE_RUNNER_APP_VERSION,
                },
            },
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': INSTANCE_TYPE,
                'disk_config': {
                    'boot_disk_type': 'pd-standard',
                    'boot_disk_size_gb': 100,
                },
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': INSTANCE_TYPE,
                'disk_config': {
                    'boot_disk_type': 'pd-standard',
                    'boot_disk_size_gb': 100,
                },
            },
            'secondary_worker_config': {
                'num_instances': Env.GCLOUD_DATAPROC_SECONDARY_WORKERS,
                'machine_type_uri': INSTANCE_TYPE,
                'disk_config': {
                    'boot_disk_type': 'pd-standard',
                    'boot_disk_size_gb': 100,
                },
                'is_preemptible': True,
                'preemptibility': 'PREEMPTIBLE',
            },
            'software_config': {
                'image_version': DEBIAN_IMAGE,
                'properties': {
                    'spark:spark.driver.maxResultSize': '0',
                    'spark:spark.task.maxFailures': '20',
                    'spark:spark.kryoserializer.buffer.max': '2g',
                    'spark:spark.driver.extraJavaOptions': '-Xss16M',
                    'spark:spark.executor.extraJavaOptions': '-Xss16M',
                    'hdfs:dfs.replication': '1',
                    'dataproc:dataproc.logging.stackdriver.enable': 'false',
                    'dataproc:dataproc.monitoring.stackdriver.enable': 'false',
                    'spark:spark.driver.memory': '41g',
                    'yarn:yarn.nodemanager.resource.memory-mb': '50585',
                    'yarn:yarn.scheduler.maximum-allocation-mb': '25292',
                    'spark:spark.executor.cores': '4',
                    'spark:spark.executor.memory': '10117m',
                    'spark:spark.executor.memoryOverhead': '15175m',
                    'spark:spark.memory.storageFraction': '0.2',
                    'spark:spark.executorEnv.HAIL_WORKER_OFF_HEAP_MEMORY_PER_CORE_MB': '6323',
                    'spark:spark.speculation': 'true',
                    'spark-env:ACCESS_PRIVATE_REFERENCE_DATASETS': '1'
                    if Env.ACCESS_PRIVATE_REFERENCE_DATASETS
                    else '0',
                    'spark-env:CHECK_SEX_AND_RELATEDNESS': '1'
                    if Env.CHECK_SEX_AND_RELATEDNESS
                    else '0',
                    'spark-env:EXPECT_WES_FILTERS': '1'
                    if Env.EXPECT_WES_FILTERS
                    else '0',
                    'spark-env:HAIL_SEARCH_DATA_DIR': Env.HAIL_SEARCH_DATA_DIR,
                    'spark-env:HAIL_TMP_DIR': Env.HAIL_TMP_DIR,
                    'spark-env:INCLUDE_PIPELINE_VERSION_IN_PREFIX': '1'
                    if Env.INCLUDE_PIPELINE_VERSION_IN_PREFIX
                    else '0',
                    'spark-env:LOADING_DATASETS_DIR': Env.LOADING_DATASETS_DIR,
                    'spark-env:PRIVATE_REFERENCE_DATASETS_DIR': Env.PRIVATE_REFERENCE_DATASETS_DIR,
                    'spark-env:REFERENCE_DATASETS_DIR': Env.REFERENCE_DATASETS_DIR,
                    'spark-env:CLINGEN_ALLELE_REGISTRY_LOGIN': Env.CLINGEN_ALLELE_REGISTRY_LOGIN,
                    'spark-env:CLINGEN_ALLELE_REGISTRY_PASSWORD': Env.CLINGEN_ALLELE_REGISTRY_PASSWORD,
                },
            },
            'lifecycle_config': {'idle_delete_ttl': {'seconds': 1200}},
            'encryption_config': {},
            'autoscaling_config': {},
            'endpoint_config': {},
            'initialization_actions': [
                {
                    'executable_file': f'gs://seqr-pipeline-runner-builds/{Env.DEPLOYMENT_TYPE}/{Env.PIPELINE_RUNNER_APP_VERSION}/bin/dataproc_vep_init.bash',
                    'execution_timeout': {'seconds': 1200},
                },
            ],
        },
    }


@luigi.util.inherits(BaseLoadingPipelineParams)
class CreateDataprocClusterTask(luigi.Task):
    # NB: The luigi.dataproc.contrib module was old and bad
    # so we built our own shim.
    run_id = luigi.Parameter()
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # https://cloud.google.com/dataproc/docs/tutorials/python-library-example
        self.client = dataproc.ClusterControllerClient(
            client_options={
                'api_endpoint': f'{Env.GCLOUD_REGION}-dataproc.googleapis.com:443'.format(
                    Env.GCLOUD_REGION,
                ),
            },
        )

    def complete(self) -> bool:
        if not self.dataset_type.requires_dataproc:
            return True
        try:
            client = self.client.get_cluster(
                request={
                    'project_id': Env.GCLOUD_PROJECT,
                    'region': Env.GCLOUD_REGION,
                    'cluster_name': f'{CLUSTER_NAME_PREFIX}-{self.reference_genome.value.lower()}',
                },
            )
        except Exception:
            return False
        else:
            return client.status.state == SUCCESS_STATE

    def run(self):
        operation = self.client.create_cluster(
            request={
                'project_id': Env.GCLOUD_PROJECT,
                'region': Env.GCLOUD_REGION,
                'cluster': get_cluster_config(self.reference_genome, self.run_id),
            },
        )
        while True:
            if operation.done():
                result = operation.result()  # Will throw on failure!
                msg = f'Created cluster {result.cluster_name} with cluster uuid: {result.cluster_uuid}'
                logger.info(msg)
                break
            logger.info('Waiting for cluster spinup')
            time.sleep(3)
