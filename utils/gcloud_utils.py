import logging
import re
import time
import sys

from utils.shell_utils import run_shell_command, FileStats, get_file_stats

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_gcloud_file_stats(gs_path):
    if gs_path.endswith(".vds"):
        gs_path += "/metadata.json.gz"  # set path to a file inside the .vds directory because gsutil stat works only on files.

    _, gsutil_stat_output, _ = run_shell_command("gsutil stat %(gs_path)s" % locals(), wait_and_return_log_output=True, verbose=False)

    """
    Example gsutil stat output:

    Creation time:          Fri, 09 Jun 2017 09:36:23 GMT
    Update time:            Fri, 09 Jun 2017 09:36:23 GMT
    Storage class:          REGIONAL
    Content-Length:         363620675
    Content-Type:           text/x-vcard
    Hash (crc32c):          SWOktA==
    Hash (md5):             fEdIumyOFR7HvULeAwXCwQ==
    ETag:                   CMae+J67sNQCEAE=
    Generation:             1497000983793478
    Metageneration:         1
    """

    if not gsutil_stat_output:
        return None

    EMPTY_MATCH_OBJ = re.match("()", "")
    DATE_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'

    creation_time = (re.search("Creation.time:[\s]+(.+)", gsutil_stat_output, re.IGNORECASE) or EMPTY_MATCH_OBJ).group(1)
    update_time = (re.search("Update.time:[\s]+(.+)", gsutil_stat_output, re.IGNORECASE) or EMPTY_MATCH_OBJ).group(1)
    file_size = (re.search("Content-Length:[\s]+(.+)", gsutil_stat_output, re.IGNORECASE) or EMPTY_MATCH_OBJ).group(1)
    file_md5 = (re.search("Hash (md5):[\s]+(.+)", gsutil_stat_output, re.IGNORECASE) or EMPTY_MATCH_OBJ).group(1)

    ctime = time.mktime(time.strptime(creation_time, DATE_FORMAT))
    mtime = time.mktime(time.strptime(update_time, DATE_FORMAT))
    return FileStats(ctime=ctime, mtime=mtime, size=file_size, md5=file_md5)


def get_local_or_gcloud_file_stats(file_path):
    if file_path.startswith("gs://"):
        file_stats = get_gcloud_file_stats(file_path)
    else:
        file_stats = get_file_stats(file_path)
    return file_stats


def _get_file_ctime(file_path):
    file_stats = get_local_or_gcloud_file_stats(file_path)
    return file_stats.ctime if file_stats else 0


def inputs_older_than_outputs(inputs, outputs, label=""):
    max_input_ctime = max(_get_file_ctime(input_path) for input_path in inputs)
    min_output_ctime = min(_get_file_ctime(output_path) for output_path in outputs)

    if max_input_ctime < min_output_ctime:
        logger.info(label + "output(s) (%s) up to date relative to input(s) (%s)" % (", ".join(outputs), ", ".join(inputs)))
    else:
        logger.info(label + "output(s) (%s) (%s) are newer than input(s) (%s) (%s)" % (", ".join(outputs), max_input_ctime, ", ".join(inputs), min_output_ctime))
    return max_input_ctime < min_output_ctime

