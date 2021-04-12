import gzip
import logging
import os
from tqdm import tqdm

from sv_pipeline.genome.utils.download_utils import download_file

GENOME_VERSION_GRCh37 = "37"
GENOME_VERSION_GRCh38 = "38"

logger = logging.getLogger(__name__)

GENCODE_GTF_URL = "http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_{gencode_release}/gencode.v{gencode_release}.annotation.gtf.gz"
GENCODE_LIFT37_GTF_URL = "http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_{gencode_release}/GRCh37_mapping/gencode.v{gencode_release}lift37.annotation.gtf.gz"

# expected GTF file header
GENCODE_FILE_HEADER = [
    'chrom', 'source', 'feature_type', 'start', 'end', 'score', 'strand', 'phase', 'info'
]


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

            for i, line in enumerate(tqdm(gencode_file, unit=' gencode records')):
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
