import hashlib
import time

import requests

from v03_pipeline.lib.logger import get_logger

HTTP_REQUEST_TIMEOUT = 5

URL = 'https://reg.genome.network/alleles?file=hgvs'

logger = get_logger(__name__)


def register_alleles(hgvs_expressions: list[str], login: str, password: str) -> None:
    # adapted from https://reg.clinicalgenome.org/doc/scripts/request_with_payload.py
    identity = hashlib.sha1((login + password).encode('utf-8')).hexdigest()
    gb_time = str(int(time.time()))
    token = hashlib.sha1((URL + identity + gb_time).encode('utf-8')).hexdigest()
    request = URL + '&gbLogin=' + login + '&gbTime=' + gb_time + '&gbToken=' + token
    res = requests.put(
        request,
        data='\n'.join(hgvs_expressions),
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    response = res.json()
    if not res.ok:
        error_type = response['errorType']
        description = response['description']
        message = response['message']
        error = f'\nAPI URL: {URL}\nTYPE: {error_type}\nDESCRIPTION: {description}\nMESSAGE: {message}'
        logger.error(error)


register_alleles(
    [
        'NC_000001.10:g.865625G>A',
        'NC_000001.10:g.865627T>C',
        'NC_000001.10:g.865628G>A',
    ],
    'jklugherz_gmail',
    'XVRJ4NzTC8YrM!t',
)
