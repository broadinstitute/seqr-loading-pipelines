import logging
import os
import requests
import tempfile
from contextlib import contextmanager
import hail as hl

logger = logging.getLogger(__name__)

def parse_gs_path_to_bucket(gs_path):
    bucket_name = gs_path.replace('gs://', '').split('/')[0]
    file_name = gs_path.split(bucket_name)[-1].lstrip('/')

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    return bucket, file_name


def stream_gs_file(gs_path, raw_download=False):
    logger.info(f'Stream from GCS: {gs_path}')
    bucket, file_name = parse_gs_path_to_bucket(gs_path)

    blob = bucket.get_blob(file_name)

    return blob and blob.download_as_string(raw_download=raw_download)

@contextmanager
def file_writer(file_path, get_existing_size=False):
    bucket = None
    size = None
    if is_gs_path(file_path):
        local_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(file_path))
        bucket, file_name = parse_gs_path_to_bucket(file_path)
        if get_existing_size:
            blob = bucket.get_blob(file_name)
            size = blob and blob.size
    else:
        local_file_path = file_path
        if get_existing_size:
            size = os.path.isfile(local_file_path) and os.path.getsize(local_file_path)

    local_file = open(local_file_path, 'wb')

    yield local_file, size

    local_file.close()

    if bucket:
        blob = bucket.blob(file_name)
        blob.upload_from_filename(local_file_path)


def is_gs_path(path):
    return path.startswith('gs://')


def path_exists(path):
    is_gs = is_gs_path(path)
    return (is_gs and hl.hadoop_exists(path)) or (not is_gs and os.path.exists(path))


DEFAULT_TO_DIR = tempfile.gettempdir()


def download_file(url, to_dir=None, verbose=True):
    """Download the given file and returns its local path.
     Args:
        url (string): HTTP or FTP url
        to_dir: optional save to directory
        verbose: display verbose information
     Returns:
        string: local file path
    """
    if to_dir is None:
        to_dir = DEFAULT_TO_DIR

    if not (url and url.startswith(("http://", "https://"))):
        raise ValueError("Invalid url: {}".format(url))
    remote_file_size = _get_remote_file_size(url)

    file_path = os.path.join(to_dir, os.path.basename(url))
    with file_writer(file_path, get_existing_size=True) as fw:
        f, file_size = fw
        if file_size and file_size == remote_file_size:
            logger.info("Re-using {} previously downloaded from {}".format(file_path, url))
            return file_path

        is_gz = url.endswith(".gz")
        response = requests.get(url, stream=is_gz)
        input_iter = response if is_gz else response.iter_content()
        if verbose:
            logger.info("Downloading {} to {}".format(url, file_path))

        f.writelines(input_iter)
        input_iter.close()

    return file_path


def _get_remote_file_size(url):
    return int(requests.head(url).headers.get('Content-Length', '0'))
