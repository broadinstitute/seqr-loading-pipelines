import os

LOCAL_DISK_MOUNT_PATH = '/var/seqr'
GRCH37_TO_GRCH38_LIFTOVER_REF_PATH = (
    'gs://hail-common/references/grch37_to_grch38.over.chain.gz'
    if os.environ.get('HAIL_DATAPROC') == '1'
    else 'v03_pipeline/var/liftover/grch37_to_grch38.over.chain.gz'
)
GRCH38_TO_GRCH37_LIFTOVER_REF_PATH = (
    'gs://hail-common/references/grch38_to_grch37.over.chain.gz'
    if os.environ.get('HAIL_DATAPROC') == '1'
    else 'v03_pipeline/var/liftover/grch38_to_grch37.over.chain.gz'
)
