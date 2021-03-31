import gzip
import logging
import tempfile
import os
from tqdm import tqdm
import requests

GENOME_VERSION_GRCh37 = "37"
GENOME_VERSION_GRCh38 = "38"

logger = logging.getLogger(__name__)

GENCODE_GTF_URL = "http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_{gencode_release}/gencode.v{gencode_release}.annotation.gtf.gz"
GENCODE_LIFT37_GTF_URL = "http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_{gencode_release}/GRCh37_mapping/gencode.v{gencode_release}lift37.annotation.gtf.gz"

# expected GTF file header
GENCODE_FILE_HEADER = [
    'chrom', 'source', 'feature_type', 'start', 'end', 'score', 'strand', 'phase', 'info'
]


def download_file(url, to_dir=tempfile.gettempdir(), verbose=True):
    """Download the given file and returns its local path.
     Args:
        url (string): HTTP or FTP url
     Returns:
        string: local file path
    """

    if not (url and url.startswith(("http://", "https://"))):
        raise ValueError("Invalid url: {}".format(url))
    local_file_path = os.path.join(to_dir, os.path.basename(url))
    remote_file_size = _get_remote_file_size(url)
    if os.path.isfile(local_file_path) and os.path.getsize(local_file_path) == remote_file_size:
        logger.info("Re-using {} previously downloaded from {}".format(local_file_path, url))
        return local_file_path

    is_gz = url.endswith(".gz")
    response = requests.get(url, stream=is_gz)
    input_iter = response if is_gz else response.iter_content()
    if verbose:
        logger.info("Downloading {} to {}".format(url, local_file_path))
        input_iter = tqdm(input_iter, unit=" data" if is_gz else " lines")

    with open(local_file_path, 'wb') as f:
        f.writelines(input_iter)

    input_iter.close()

    return local_file_path


def _get_remote_file_size(url):
    if url.startswith("http"):
        response = requests.head(url)
        return int(response.headers.get('Content-Length', '0'))
    else:
        return 0  # file size not yet implemented for FTP and other protocols


def load_gencode(gencode_release, gencode_gtf_path=None, genome_version=None):
    """Update GeneInfo and TranscriptInfo tables.

    Args:
        gencode_release (int): the gencode release to load (eg. 25)
        gencode_gtf_path (str): optional local file path of gencode GTF file. If not provided, it will be downloaded.
        genome_version (str): '37' or '38'. Required only if gencode_gtf_path is specified.
    """
    if gencode_gtf_path and genome_version and os.path.isfile(gencode_gtf_path):
        if gencode_release == 19 and genome_version != GENOME_VERSION_GRCh37:
            raise Exception("Invalid genome_version: {}. gencode v19 only has a GRCh37 version".format(genome_version))
        elif gencode_release <= 22 and genome_version != GENOME_VERSION_GRCh38:
            raise Exception("Invalid genome_version: {}. gencode v20, v21, v22 only have a GRCh38 version".format(genome_version))
        elif genome_version != GENOME_VERSION_GRCh38 and "lift" not in gencode_gtf_path.lower():
            raise Exception("Invalid genome_version for file: {}. gencode v23 and up must have 'lift' in the filename or genome_version arg must be GRCh38".format(gencode_gtf_path))

        gencode_gtf_paths = {genome_version: gencode_gtf_path}
    elif gencode_gtf_path and not genome_version:
        raise Exception("The genome version must also be specified after the gencode GTF file path")
    else:
        if gencode_release == 19:
            urls = [('37', GENCODE_GTF_URL.format(gencode_release=gencode_release))]
        elif gencode_release <= 22:
            urls = [('38', GENCODE_GTF_URL.format(gencode_release=gencode_release))]
        else:
            urls = [
                ('37', GENCODE_LIFT37_GTF_URL.format(gencode_release=gencode_release)),
                ('38', GENCODE_GTF_URL.format(gencode_release=gencode_release)),
            ]
        gencode_gtf_paths = {}
        for genome_version, url in urls:
            local_filename = download_file(url)
            gencode_gtf_paths.update({genome_version: local_filename})

    gene_id_mapping = {}

    for genome_version, gencode_gtf_path in gencode_gtf_paths.items():

        logger.info("Loading {} (genome version: {})".format(gencode_gtf_path, genome_version))
        with gzip.open(gencode_gtf_path, 'rt') as gencode_file:

            for line in tqdm(gencode_file, unit=' gencode records'):
                line = line.rstrip('\r\n')
                if not line or line.startswith('#'):
                    continue
                fields = line.split('\t')

                if len(fields) != len(GENCODE_FILE_HEADER):
                    raise ValueError("Unexpected number of fields on line #%s: %s" % (i, fields))

                record = dict(zip(GENCODE_FILE_HEADER, fields))

                if record['feature_type'] != 'gene':
                    continue

                # parse info field
                info_fields = [x.strip().split() for x in record['info'].split(';') if x != '']
                info_fields = {k: v.strip('"') for k, v in info_fields}

                gene_id_mapping[info_fields['gene_name']] = info_fields["gene_id"]

    logger.info('Get {} gene id mapping records'.format(len(gene_id_mapping)))
    return gene_id_mapping
