import dataclasses
import hashlib
import time

import requests

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env

URL = 'https://reg.genome.network/alleles?file=ExAC.id'

HTTP_REQUEST_TIMEOUT = 120
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


def register_alleles(variant_ids: list[str], url: str = URL) -> None:
    logger.info(
        f'Registering {len(variant_ids)} alleles to the Clingen Allele Registry',
    )
    login = Env.ALLELE_REGISTRY_LOGIN
    password = Env.ALLELE_REGISTRY_PASSWORD

    # adapted from https://reg.clinicalgenome.org/doc/scripts/request_with_payload.py
    identity = hashlib.sha1((login + password).encode('utf-8')).hexdigest()  # noqa: S324
    gb_time = str(int(time.time()))
    token = hashlib.sha1((url + identity + gb_time).encode('utf-8')).hexdigest()  # noqa: S324
    request = url + '&gbLogin=' + login + '&gbTime=' + gb_time + '&gbToken=' + token

    for i in range(0, len(variant_ids), MAX_REQUEST_SIZE):
        chunk = variant_ids[i : i + MAX_REQUEST_SIZE]
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
                f'\nDESCRIPTION: {error.description}\nMESSAGE: {error.message}',
            )
            res.raise_for_status()

        errors = [
            AlleleRegistryError.from_api_response(allele_response)
            for allele_response in response
            if 'errorType' in allele_response
        ]
        if errors:
            error = errors[0]
            logger.warning(
                f'{len(errors)} alleles failed to register.'
                f'\nAPI URL: {url}\nTYPE: {error.error_type}'
                f'\nDESCRIPTION: {error.description}\nMESSAGE: {error.message}'
                f'\nINPUT_LINE: {error.input_line}',
            )
