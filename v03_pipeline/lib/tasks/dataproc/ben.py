import hail as hl
from google.cloud import dataproc_v1 as dataproc
from pip._internal.operations import freeze as pip_freeze

DEBIAN_IMAGE = '2.1.33-debian11'
HAIL_VERSION = hl.version().split('-')[0]
INSTANCE_TYPE = 'n1-highmem-8'
PKGS = '|'.join(pip_freeze.freeze())
SUCCESS_STATE = 'RUNNING'

CONFIG = {
    'project_id': 'seqr-project',
    'cluster_name': 'bentest-1',
    'config': {
        'gce_cluster_config': {
            'zone_uri': 'us-central1-b',
            'metadata': {
                'WHEEL': 'gs://hail-common/hailctl/dataproc/0.2.132/hail-0.2.132-py3-none-any.whl',
                'PKGS': PKGS,
                'ENVIRONMENT': 'dev',
                'DEPLOYMENT_TYPE': 'dev',
                'REFERENCE_GENOME': 'GRCh38',
                'PIPELINE_RUNNER_APP_VERSION': 'latest',
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
            'num_instances': 5,
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
                'spark-env:ACCESS_PRIVATE_REFERENCE_DATASETS': '1',
                'spark-env:CHECK_SEX_AND_RELATEDNESS': '1',
                'spark-env:EXPECT_WES_FILTERS': '1',
                'spark-env:HAIL_SEARCH_DATA_DIR': 'gs://seqr-hail-search-data',
                'spark-env:HAIL_TMP_DIR': 'gs://seqr-scratch-temp',
                'spark-env:INCLUDE_PIPELINE_VERSION_IN_PREFIX': '1',
                'spark-env:LOADING_DATASETS_DIR': 'gs://seqr-loading-temp',
                'spark-env:PRIVATE_REFERENCE_DATASETS_DIR': 'gs://seqr-reference-data-private',
                'spark-env:REFERENCE_DATASETS_DIR': 'gs://seqr-reference-data',
                'spark-env:CLINGEN_ALLELE_REGISTRY_LOGIN': 'abc',
                'spark-env:CLINGEN_ALLELE_REGISTRY_PASSWORD': '123',
            },
        },
        'lifecycle_config': {'idle_delete_ttl': {'seconds': 1200}},
        'encryption_config': {},
        'autoscaling_config': {},
        'endpoint_config': {},
        'initialization_actions': [
            {
                'executable_file': 'gs://seqr-pipeline-runner-builds/dev/latest/bin/dataproc_vep_init.bash',
                'execution_timeout': {'seconds': 1200},
            },
        ],
    },
}

client = dataproc.ClusterControllerClient(
    client_options={
        'api_endpoint': 'us-central1-dataproc.googleapis.com:443',
    },
)

operation = client.create_cluster(
    request={
        'project_id': 'seqr-project',
        'region': 'us-central1',
        'cluster': CONFIG,
    },
)
