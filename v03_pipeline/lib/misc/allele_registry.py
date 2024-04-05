import hashlib
import time

import requests

from v03_pipeline.lib.logger import get_logger

URL = 'https://reg.genome.network/alleles?file=hgvs'
SEQR_TEAM_LOGIN = 'seqr_team'

HTTP_REQUEST_TIMEOUT = 5
MAX_REQUEST_SIZE = 2000
logger = get_logger(__name__)


def register_alleles(hgvs_expressions: list[str], password: str = None) -> None:
    # adapted from https://reg.clinicalgenome.org/doc/scripts/request_with_payload.py
    identity = hashlib.sha1((SEQR_TEAM_LOGIN + password).encode('utf-8')).hexdigest()
    gb_time = str(int(time.time()))
    token = hashlib.sha1((URL + identity + gb_time).encode('utf-8')).hexdigest()
    request = (
        URL + '&gbLogin=' + SEQR_TEAM_LOGIN + '&gbTime=' + gb_time + '&gbToken=' + token
    )

    for i in range(0, len(hgvs_expressions), MAX_REQUEST_SIZE):
        chunk = hgvs_expressions[i : i + MAX_REQUEST_SIZE]
        res = requests.put(
            request,
            data='\n'.join(chunk),
            timeout=HTTP_REQUEST_TIMEOUT,
        )
        response = res.json()
        if not res.ok:
            error_type = response['errorType']
            description = response['description']
            message = response['message']
            error = f'\nAPI URL: {URL}\nTYPE: {error_type}\nDESCRIPTION: {description}\nMESSAGE: {message}'
            logger.error(error)
