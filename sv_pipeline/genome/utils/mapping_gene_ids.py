import gzip
import logging
import os
import pickle
from tqdm import tqdm
import hail as hl

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

gene_id_mapping = {}


def load_gtf_data(gene_id_mapping, gencode_gtf_path):
    root, ext = os.path.splitext(gencode_gtf_path)
    pickle_file = root + '.pickle'
    if os.path.isfile(pickle_file):
        logger.info('Use the existing pickle file {}.\nIf you want to reload the data, please delete it and re-run'.format(pickle_file))
        handle = open(pickle_file, 'rb')
        p = pickle.load(handle)
        handle.close()
        gene_id_mapping.update(p)
        return

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

    handle = open(pickle_file, 'wb')
    pickle.dump(gene_id_mapping, handle, protocol=pickle.HIGHEST_PROTOCOL)
    handle.close()


# for performance comparison. It seems that hail search with filtering is slow. And converting hail table to Python JSON is also slow.
def hail_load_gtf_data(gene_id_mapping, gencode_gtf_path):
    gtf_table = hl.import_table(gencode_gtf_path, force=True, comment=['#'], no_header=True)
    gtf_table = gtf_table.filter(gtf_table.f2 == 'gene')
    info_fields = gtf_table.f8.split('; ').filter(lambda x: x.startswith('gene_id ') | x.startswith('gene_name ')).map(lambda x: x.split(' '))
    gtf_table = gtf_table.annotate(gene_id_mapping=(info_fields[1][1].replace('"', ''),
                                                    info_fields[0][1].replace('"', '')))
    gene_id_mapping.update(hl.eval(hl.dict(gtf_table.gene_id_mapping.collect())))


def load_gencode(gencode_release, gencode_gtf_path=None, genome_version=None, download_path=None):
    """Update GeneInfo and TranscriptInfo tables.

    Args:
        gencode_release (int): the gencode release to load (eg. 25)
        gencode_gtf_path (str): optional local file path of gencode GTF file. If not provided, it will be downloaded.
        genome_version (str): '38'. Required only if gencode_gtf_path is specified.
        download_path (str): The path for downloaded data
    """
    global gene_id_mapping
    gene_id_mapping = {}

    if gencode_gtf_path and genome_version and os.path.isfile(gencode_gtf_path):
        if gencode_release <= 22:
            raise Exception("Invalid genome_version: {}. Only gencode v23 and up is supported".format(genome_version))
        elif genome_version != GENOME_VERSION_GRCh38:
            raise Exception("Invalid genome_version for file: {}. Genome_version arg must be GRCh38".format(gencode_gtf_path))
    elif gencode_gtf_path and not genome_version:
        raise Exception("The genome version must also be specified after the gencode GTF file path")
    else:
        url = GENCODE_GTF_URL.format(gencode_release=gencode_release)
        local_filename = download_file(url, to_dir=download_path)
        gencode_gtf_path = local_filename
        genome_version = GENOME_VERSION_GRCh38

    logger.info("Loading {} (genome version: {})".format(gencode_gtf_path, genome_version))
    load_gtf_data(gene_id_mapping, gencode_gtf_path)

    logger.info('Get {} gene id mapping records'.format(len(gene_id_mapping)))
    gene_id_mapping = hl.literal(gene_id_mapping)


def get_gene_id(gene_symbol):
    return gene_id_mapping[gene_symbol]
