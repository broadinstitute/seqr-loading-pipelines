import dataclasses
import hashlib
import math
import tempfile
import time

import hail as hl
import requests
from requests import HTTPError

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env, ReferenceGenome

MAX_VARIANTS_PER_REQUEST = 1000000
ALLELE_REGISTRY_URL = 'https://reg.genome.network/alleles?file=vcf&fields=none+@id'
HTTP_REQUEST_TIMEOUT = 300

logger = get_logger(__name__)


@dataclasses.dataclass
class AlleleRegistryError:
    base_url: str
    error_type: str
    description: str
    message: str
    input_line: str | None

    @classmethod
    def from_api_response(cls, response: dict, base_url: str):
        return cls(
            base_url=base_url,
            error_type=response['errorType'],
            description=response['description'],
            message=response['message'],
            input_line=response.get('inputLine'),
        )

    @property
    def loggable_message(self) -> str:
        msg = (
            f'\nAPI URL: {self.base_url}\nTYPE: {self.error_type}'
            f'\nDESCRIPTION: {self.description}\nMESSAGE: {self.message}'
        )
        return (
            msg if self.input_line is None else f'{msg}\nINPUT_LINE: {self.input_line}'
        )


def register_alleles_in_chunks(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    base_url: str = ALLELE_REGISTRY_URL,
    chunk_size: int = MAX_VARIANTS_PER_REQUEST,
):
    num_rows = ht.count()
    num_chunks = math.ceil(num_rows / chunk_size)
    logger.info(
        f'Registering {num_rows} allele(s) in chunks of {chunk_size} in {num_chunks} request(s).',
    )
    for start_idx in range(0, num_rows, chunk_size):
        end_idx = start_idx + chunk_size
        if end_idx == chunk_size:
            chunk_ht = ht.head(chunk_size)
        elif end_idx <= num_rows:
            chunk_ht = ht.head(end_idx).tail(chunk_size)
        else:
            chunk_ht = ht.tail(end_idx - num_rows)
        register_alleles(chunk_ht, reference_genome, base_url)


def register_alleles(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    base_url: str,
) -> None:
    with tempfile.NamedTemporaryFile(
        suffix='.vcf',
    ) as raw_vcf, tempfile.NamedTemporaryFile(suffix='.vcf') as formatted_vcf:
        hl.export_vcf(ht, raw_vcf.name)

        # Reformat the VCF created by hail's 'export_vcf' function: replace the header and remove any 'chr' prefix
        with open(raw_vcf.name) as vcf_in, open(formatted_vcf.name, 'w') as vcf_out:
            vcf_out.writelines(reference_genome.allele_registry_vcf_header)
            for line in vcf_in:
                if not line.startswith('#'):
                    vcf_out.write(line.replace('chr', ''))

        logger.info('Calling the Clingen Allele Registry.')
        with open(formatted_vcf.name) as vcf_in:
            data = vcf_in.read()
            res = requests.put(
                url=build_url(base_url),
                data=data,
                timeout=HTTP_REQUEST_TIMEOUT,
            )
            handle_api_response(res, base_url)


def build_url(base_url: str) -> str:
    login = Env.ALLELE_REGISTRY_LOGIN
    password = Env.ALLELE_REGISTRY_PASSWORD

    if login is None or password is None:
        msg = 'Please set the ALLELE_REGISTRY_LOGIN and ALLELE_REGISTRY_PASSWORD environment variables.'
        raise ValueError(msg)

    # adapted from https://reg.clinicalgenome.org/doc/scripts/request_with_payload.py
    identity = hashlib.sha1((login + password).encode('utf-8')).hexdigest()  # noqa: S324
    gb_time = str(int(time.time()))
    token = hashlib.sha1((base_url + identity + gb_time).encode('utf-8')).hexdigest()  # noqa: S324
    return base_url + '&gbLogin=' + login + '&gbTime=' + gb_time + '&gbToken=' + token


def handle_api_response(res: requests.Response, base_url: str) -> None:
    response = res.json()
    if not res.ok or 'errorType' in response:
        error = AlleleRegistryError.from_api_response(response, base_url)
        logger.error(error.loggable_message)
        raise HTTPError(error.message)

    errors = [
        AlleleRegistryError.from_api_response(allele_response, base_url)
        for allele_response in response
        if 'errorType' in allele_response
    ]
    logger.info(
        f'{len(response) - len(errors)} out of {len(response)} returned CAID(s).',
    )
    if errors:
        logger.warning(
            f'{len(errors)} failed. First error: {errors[0].loggable_message}',
        )
