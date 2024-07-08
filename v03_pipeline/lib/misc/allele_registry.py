import dataclasses
import hashlib
import json
import math
import time
import uuid

import hail as hl
import hailtop.fs as hfs
import requests
from google.cloud import secretmanager
from requests import HTTPError

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env, ReferenceGenome

MAX_VARIANTS_PER_REQUEST = 1000000
ALLELE_REGISTRY_URL = 'https://reg.genome.network/alleles?file=vcf&fields=none+@id+genomicAlleles+externalRecords.{}.id'
HTTP_REQUEST_TIMEOUT_S = 420

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

    def __str__(self) -> str:
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
        yield register_alleles(chunk_ht, reference_genome, base_url)


def register_alleles(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    base_url: str,
) -> hl.Table:
    uuid4 = uuid.uuid4()
    raw_vcf_file_name = f'{Env.HAIL_TMPDIR}/r_{uuid4}.vcf'
    formatted_vcf_file_name = f'{Env.HAIL_TMPDIR}/f_{uuid4}.vcf'

    # Export the variants to a VCF
    hl.export_vcf(ht, raw_vcf_file_name)

    # Reformat the VCF created by hail's 'export_vcf' function to be compatible with the Allele Registry
    with hfs.open(raw_vcf_file_name, 'r') as vcf_in, hfs.open(
        formatted_vcf_file_name,
        'w',
    ) as vcf_out:
        vcf_out.writelines(reference_genome.allele_registry_vcf_header)
        for line in vcf_in:
            if not line.startswith('#'):
                # NB: The Allele Registry does not accept contigs prefixed with 'chr', even for GRCh38
                vcf_out.write(line.replace('chr', ''))

    logger.info('Calling the ClinGen Allele Registry')
    with hfs.open(formatted_vcf_file_name, 'r') as vcf_in:
        data = vcf_in.read()
        res = requests.put(
            url=build_url(base_url, reference_genome),
            data=data,
            timeout=HTTP_REQUEST_TIMEOUT_S,
        )
        return handle_api_response(res, base_url, reference_genome)


def build_url(base_url: str, reference_genome: ReferenceGenome) -> str:
    login, password = get_ar_credentials_from_secret_manager()

    # Request a gnomad ID for the correct reference genome
    base_url = base_url.format(reference_genome.allele_registry_gnomad_id)

    # adapted from https://reg.clinicalgenome.org/doc/scripts/request_with_payload.py
    identity = hashlib.sha1((login + password).encode('utf-8')).hexdigest()  # noqa: S324
    gb_time = str(int(time.time()))
    token = hashlib.sha1((base_url + identity + gb_time).encode('utf-8')).hexdigest()  # noqa: S324
    return base_url + '&gbLogin=' + login + '&gbTime=' + gb_time + '&gbToken=' + token


def get_ar_credentials_from_secret_manager() -> tuple[str, str]:
    if Env.ALLELE_REGISTRY_SECRET_NAME is None:
        msg = (
            'SHOULD_REGISTER_ALLELES is True but cannot get allele registry credentials '
            'because ALLELE_REGISTRY_SECRET_NAME is not set'
        )
        raise ValueError(msg)

    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(
        Env.PROJECT_ID,
        Env.ALLELE_REGISTRY_SECRET_NAME,
        'latest',
    )
    response = client.access_secret_version(request={'name': name})
    payload_dict = json.loads(response.payload.data.decode('UTF-8'))
    return payload_dict['login'], payload_dict['password']


def handle_api_response(  # noqa: C901
    res: requests.Response,
    base_url: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    response = res.json()
    if not res.ok or 'errorType' in response:
        error = AlleleRegistryError.from_api_response(response, base_url)
        logger.error(error)
        raise HTTPError(error.message)

    parsed_structs = []
    errors = []
    unmappable_variants = []
    for allele_response in response:
        if 'errorType' in allele_response:
            errors.append(
                AlleleRegistryError.from_api_response(allele_response, base_url),
            )
            continue

        # Extract CAID and allele info
        try:
            caid = allele_response['@id'].split('/')[-1]
            allele_info = next(
                record
                for record in allele_response['genomicAlleles']
                if record['referenceGenome'] == reference_genome.value
            )
            chrom = allele_info['chromosome']
            pos = allele_info['coordinates'][0]['end']
            ref = allele_info['coordinates'][0]['referenceAllele']
            alt = allele_info['coordinates'][0]['allele']
        except (KeyError, StopIteration):
            unmappable_variants.append(allele_response)
            continue

        if ref == '' or alt == '':
            # AR will turn alleles like ["A","ATT"] to ["", "TT"] so try using gnomad IDs instead
            if 'externalRecords' in allele_response:
                gnomad_id = allele_response['externalRecords'][
                    reference_genome.allele_registry_gnomad_id
                ][0]['id']
                chrom, pos, ref, alt = gnomad_id.split('-')
            else:
                unmappable_variants.append(allele_response)
                continue

        formatted_chromosome = chrom
        if reference_genome == ReferenceGenome.GRCh38:
            formatted_chromosome = f'chr{chrom}'
            if chrom == 'MT':
                formatted_chromosome = 'chrM'

        struct = hl.Struct(
            locus=hl.Locus(
                formatted_chromosome,
                int(pos),
                reference_genome=reference_genome.value,
            ),
            alleles=[ref, alt],
            CAID=caid,
        )
        parsed_structs.append(struct)

    logger.info(
        f'{len(response) - len(errors)} out of {len(response)} variants returned CAID(s)',
    )
    if unmappable_variants:
        logger.info(
            f'{len(unmappable_variants)} registered variant(s) cannot be mapped back to ours. '
            f'\nFirst unmappable variant:\n{unmappable_variants[0]}',
        )
    if errors:
        logger.warning(
            f'{len(errors)} failed. First error: {errors[0]}',
        )
    return hl.Table.parallelize(
        parsed_structs,
        hl.tstruct(
            locus=hl.tlocus(reference_genome.value),
            alleles=hl.tarray(hl.tstr),
            CAID=hl.tstr,
        ),
        key=('locus', 'alleles'),
    )
