import dataclasses
import hashlib
import math
import os
import tempfile
import time

import hail as hl
import requests
from requests import HTTPError

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env, ReferenceGenome
from v03_pipeline.lib.reference_data.clinvar import safely_move_to_gcs

MAX_VARIANTS_PER_REQUEST = 1000000
ALLELE_REGISTRY_URL = 'https://reg.genome.network/alleles?file=vcf&fields=none+@id+externalRecords.gnomAD_4.id'
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
    use_gcs_filesystem: bool = True,
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
        yield register_alleles(chunk_ht, reference_genome, base_url, use_gcs_filesystem)


def register_alleles(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    base_url: str,
    use_gcs_filesystem: bool,
) -> hl.Table:
    with tempfile.NamedTemporaryFile(
        suffix='.vcf',
    ) as raw_vcf, tempfile.NamedTemporaryFile(suffix='.vcf') as formatted_vcf:
        # Export the variants to a VCF
        hl.export_vcf(ht, raw_vcf.name)

        if use_gcs_filesystem:
            raw_vcf_file_name = os.path.join(
                Env.HAIL_TMPDIR,
                os.path.basename(raw_vcf.name),
            )
            safely_move_to_gcs(raw_vcf.name, raw_vcf_file_name)
        else:
            raw_vcf_file_name = raw_vcf.name

        # Reformat the VCF created by hail's 'export_vcf' function: replace the header and remove any 'chr' prefix
        with open(raw_vcf_file_name) as vcf_in, open(
            formatted_vcf.name,
            'w',
        ) as vcf_out:
            vcf_out.writelines(reference_genome.allele_registry_vcf_header)
            for line in vcf_in:
                if not line.startswith('#'):
                    vcf_out.write(line.replace('chr', ''))

        if use_gcs_filesystem:
            formatted_vcf_file_name = os.path.join(
                Env.HAIL_TMPDIR,
                os.path.basename(raw_vcf.name),
            )
            safely_move_to_gcs(formatted_vcf.name, formatted_vcf_file_name)
        else:
            formatted_vcf_file_name = formatted_vcf.name

        logger.info('Calling the Clingen Allele Registry.')
        with open(formatted_vcf_file_name) as vcf_in:
            data = vcf_in.read()
            res = requests.put(
                url=build_url(base_url),
                data=data,
                timeout=HTTP_REQUEST_TIMEOUT,
            )
            return handle_api_response(res, base_url, reference_genome)


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


def handle_api_response(
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
    no_external_id_count = 0
    for allele_response in response:
        if 'errorType' in allele_response:
            errors.append(
                AlleleRegistryError.from_api_response(allele_response, base_url),
            )
        else:
            # Example allele_response:
            # {'@id': 'http://reg.genome.network/allele/CA520798109',
            #  'externalRecords': {'gnomAD_4': [{'id': '1-10109-AACCCT-A'}]}}
            caid = allele_response['@id'].split('/')[-1]
            if 'externalRecords' in allele_response:
                gnomad_id = allele_response['externalRecords']['gnomAD_4'][0]['id']
                chrom, pos, ref, alt = gnomad_id.split('-')
                struct = hl.Struct(
                    locus=hl.Locus(
                        f'chr{chrom}'
                        if reference_genome == ReferenceGenome.GRCh38
                        else chrom,
                        int(pos),
                        reference_genome=reference_genome.value,
                    ),
                    alleles=[ref, alt],
                    CAID=caid,
                )
                parsed_structs.append(struct)
            else:
                no_external_id_count += 1

    logger.info(
        f'{len(response) - len(errors)} out of {len(response)} returned CAID(s).',
    )
    logger.info(
        f'{no_external_id_count} registered allele(s) cannot be mapped back to our variants in the annotations table.',
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
