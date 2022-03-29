import logging
import os
import requests
import tempfile
from contextlib import contextmanager
import hail as hl
from tqdm import tqdm

logger = logging.getLogger(__name__)

@contextmanager
def file_writer(file_path):
    is_gs = is_gs_path(file_path)
    if is_gs:
        local_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(file_path))
    else:
        local_file_path = file_path
    local_file = open(local_file_path, 'wb')

    yield local_file_path, local_file

    local_file.close()
    if is_gs:
        os.system(f'gsutil mv {local_file_path} {file_path}')

def is_gs_path(path):
    return path.startswith('gs://')


def path_exists(path):
    is_gs = is_gs_path(path)
    return (is_gs and hl.hadoop_exists(path)) or (not is_gs and os.path.exists(path))


def download_file(url, to_dir=tempfile.gettempdir(), verbose=True):
    """Download the given file and returns its local path.
     Args:
        url (string): HTTP or FTP url
     Returns:
        string: local file path
    """

    if not (url and url.startswith(("http://", "https://"))):
        raise ValueError("Invalid url: {}".format(url))
    remote_file_size = _get_remote_file_size(url)

    file_path = os.path.join(to_dir, filename)
    with file_writer(file_path) as local_file_path, f:
        if os.path.isfile(local_file_path) and os.path.getsize(local_file_path) == remote_file_size:
            logger.info("Re-using {} previously downloaded from {}".format(local_file_path, url))
            return local_file_path

        is_gz = url.endswith(".gz")
        response = requests.get(url, stream=is_gz)
        input_iter = response if is_gz else response.iter_content()
        if verbose:
            logger.info("Downloading {} to {}".format(url, local_file_path))
            input_iter = tqdm(input_iter, unit=" data" if is_gz else " lines")

        f.writelines(input_iter)
        input_iter.close()

    return file_path


def _get_remote_file_size(url):
    return int(requests.head(url).headers.get('Content-Length', '0'))
