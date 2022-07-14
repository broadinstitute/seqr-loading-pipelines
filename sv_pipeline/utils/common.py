import logging
import subprocess
from datetime import datetime
from google.cloud import storage

logger = logging.getLogger(__name__)

CHROMOSOMES = [
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21',
    '22', 'X', 'Y', 'M',
]
CHROM_TO_XPOS_OFFSET = {chrom: (1 + i)*int(1e9) for i, chrom in enumerate(CHROMOSOMES)}

GS_SAMPLE_PATH = 'gs://seqr-datasets/v02/GRCh38/RDG_{sample_type}_Broad_Internal/base/projects/{project_guid}/{project_guid}_{file_ext}'


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


def _get_gs_samples(project_guid, file_ext, sample_type, expected_header, filename=None):
    """
    Get sample metadata from files in google cloud

    :param project_guid: seqr project identifier
    :param file_ext: extension for the desired sample file
    :param sample_type: sample type (WES/WGS)
    :param expected_header: expected header to validate file
    :return: parsed data from the sample file as a list of lists
    """
    file = GS_SAMPLE_PATH.format(project_guid=project_guid, sample_type=sample_type, file_ext=file_ext) if not filename else filename
    file_content = stream_gs_file(file)
    if not file_content:
        return None
    rows = file_content.split(b'\n')
    header = rows[0].decode('utf-8')
    if header.strip() != expected_header:
        raise Exception('Missing header for sample file, expected "{}" but found {}'.format(
            expected_header, header))
    return [line.decode('utf-8').strip().split('\t') for line in rows[1:] if line]


def get_sample_subset(project_guid, sample_type, filename=None):
    """
    Get sample id subset for a given project

    :param project_guid: seqr project identifier
    :param sample_type: sample type (WES/WGS)
    :return: set of sample ids
    """
    subset = _get_gs_samples(project_guid, file_ext='ids.txt', sample_type=sample_type, expected_header='s', filename=filename)
    if not subset:
        raise Exception('No sample subset file found')
    return {row[0] for row in subset}


def get_sample_remap(project_guid, sample_type):
    """
    Get an optional remapping for sample ids in the given project

    :param project_guid: seqr project identifier
    :param sample_type: sample type (WES/WGS)
    :return: dictionary mapping VCF sample ids to seqr sample ids, or None if no mapping available
    """
    remap = _get_gs_samples(project_guid, file_ext='remap.tsv', sample_type=sample_type, expected_header='s\tseqr_id')
    if remap:
        remap = {row[0]: row[1] for row in remap}
    return remap


def get_es_index_name(project, meta):
    """
    Get the name for the output ES index

    :param project: seqr project identifier
    :param meta: index metadata
    :return: index name
    """
    return '{project}__structural_variants__{sample_type}__grch{genome_version}__{datestamp}'.format(
        project=project,
        sample_type=meta['sampleType'],
        genome_version=meta['genomeVersion'],
        datestamp=datetime.today().strftime('%Y%m%d'),
    ).lower()
