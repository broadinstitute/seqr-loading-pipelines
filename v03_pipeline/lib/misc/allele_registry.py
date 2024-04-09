import dataclasses
import hashlib
import time

import requests

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env

URL = 'https://reg.genome.network/alleles?file=hgvs'
HTTP_REQUEST_TIMEOUT = 5
MAX_REQUEST_SIZE = 2000

logger = get_logger(__name__)


@dataclasses.dataclass
class AlleleRegistryError:
    error_type: str
    description: str
    message: str
    input_line: str | None

    @classmethod
    def from_api_response(cls, response: dict):
        return cls(
            error_type=response['errorType'],
            description=response['description'],
            message=response['message'],
            input_line=response.get('inputLine'),
        )


def register_alleles(hgvs_expressions: list[str], url: str = URL) -> None:
    logger.info(
        f'Registering {len(hgvs_expressions)} alleles to the Clingen Allele Registry',
    )
    login = Env.ALLELE_REGISTRY_LOGIN
    password = Env.ALLELE_REGISTRY_PASSWORD
    # adapted from https://reg.clinicalgenome.org/doc/scripts/request_with_payload.py
    identity = hashlib.sha1((login + password).encode('utf-8')).hexdigest()  # noqa: S324
    gb_time = str(int(time.time()))
    token = hashlib.sha1((url + identity + gb_time).encode('utf-8')).hexdigest()  # noqa: S324
    request = url + '&gbLogin=' + login + '&gbTime=' + gb_time + '&gbToken=' + token

    for i in range(0, len(hgvs_expressions), MAX_REQUEST_SIZE):
        chunk = hgvs_expressions[i : i + MAX_REQUEST_SIZE]
        res = requests.put(
            request,
            data='\n'.join(chunk),
            timeout=HTTP_REQUEST_TIMEOUT,
        )
        response = res.json()
        if not res.ok:
            error = AlleleRegistryError.from_api_response(response)
            logger.error(
                f'\nAPI URL: {url}\nTYPE: {error.error_type}'
                f'\nDESCRIPTION: {error.description}r\nMESSAGE: {error.message}',
            )
            res.raise_for_status()

        errors = [
            AlleleRegistryError.from_api_response(allele_response)
            for allele_response in response
            if 'errorType' in allele_response
        ]
        if errors:
            messages = ', '.join(
                [error.input_line + ': ' + error.message for error in errors],
            )
            logger.warning(
                f'{len(errors)} alleles failed to register to {url}. {messages}',
            )
