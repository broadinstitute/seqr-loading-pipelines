import argparse
import logging
import json
import tempfile
import os
import zipfile
import urllib

import hail as hl

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level='INFO')
logger = logging.getLogger(__name__)

contig_recoding={'1': 'chr1', '10': 'chr10', '11': 'chr11', '12': 'chr12', '13': 'chr13', '14': 'chr14', '15': 'chr15',
 '16': 'chr16', '17': 'chr17', '18': 'chr18', '19': 'chr19', '2': 'chr2', '20': 'chr20', '21': 'chr21', '22': 'chr22',
 '3': 'chr3', '4': 'chr4', '5': 'chr5', '6': 'chr6', '7': 'chr7', '8': 'chr8', '9': 'chr9', 'X': 'chrX', 'Y': 'chrY',
 'MT': 'chrM', 'NW_009646201.1': 'chr1'}


def get_tempfile(**kwargs):
    _, filename = tempfile.mkstemp(**kwargs)
    return filename


def convert_json2tsv(json_fname):
    with open(json_fname, 'r') as f:
        data = json.load(f)
    tsv_fname = get_tempfile(suffix='.tsv', text=True)
    with open(tsv_fname, 'w') as f:
        header = '\t'.join(data[0].keys())
        f.write(header + '\n')
        for row in data:
            f.write('\t'.join([str(v) for v in row.values()]) + '\n')
    return tsv_fname


def download_file(path):
    if path.startswith('gs://'):
        path = f'https://storage.googleapis.com/{urllib.parse.quote(path[5:])}'
    filename = get_tempfile(suffix=path.split('/')[-1])
    urllib.request.urlretrieve(path, filename)
    if path.endswith('.zip'):
        unzip_file = path.split('/')[-1][:-4]
        unzip_path = os.path.dirname(filename)
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extract(unzip_file, path=unzip_path)
        os.remove(filename)
        filename = os.path.join(unzip_path, unzip_file)
    return filename


def load_ht(config, force_write=True):
    tsv_fname = None

    logger.info(f'Downloading dataset from {config["input_path"]}.')
    dn_fname = download_file(config['input_path'])

    logger.info(f'Loading hail table from {dn_fname}.')
    types = config['field_types'] if config.get('field_types') else {}
    if config['input_type'] == 'tsv':
        ht = hl.import_table(dn_fname, types=types)
    elif config['input_type'] == 'vcf':
        ht = hl.import_vcf(dn_fname, force_bgz=True, contig_recoding=contig_recoding).rows()
    elif config['input_type'] == 'json':
        tsv_fname = convert_json2tsv(dn_fname)
        ht = hl.import_table(tsv_fname, types=types)
    else:  # assume unspecified as an ht (hail table) type.
        ht = hl.read_table(dn_fname)

    if config.get('annotate'):
        ht = ht.annotate(**{field: func(ht) for field, func in config['annotate'].items()})

    ht = ht.filter(ht.locus.contig == 'chrM')

    ht = ht.key_by('locus', 'alleles')

    logger.info(f'Writing hail table to {config["output_path"]}.')
    ht.write(config['output_path'], overwrite=force_write)

    if tsv_fname:
        os.remove(tsv_fname)
    if dn_fname != config['input_path']:
        os.remove(dn_fname)

    logger.info('Done')


def load(config):
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--force-write', help='Force write to an existing output file', action='store_true')
    args = parser.parse_args()

    # If there are out-of-memory error, set the environment variable with the following command
    # $ export PYSPARK_SUBMIT_ARGS="--driver-memory 4G pyspark-shell"
    # "4G" in the environment variable can be bigger if your computer has a larger memory.
    hl.init(default_reference='GRCh38', min_block_size=128, master='local[32]')

    load_ht(config, args.force_write)
