import argparse
import logging
import json
import tqdm
import tempfile
import os
import zipfile
import requests

import hail as hl

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level='INFO')
logger = logging.getLogger(__name__)

contig_recoding={'1': 'chr1', '10': 'chr10', '11': 'chr11', '12': 'chr12', '13': 'chr13', '14': 'chr14', '15': 'chr15',
 '16': 'chr16', '17': 'chr17', '18': 'chr18', '19': 'chr19', '2': 'chr2', '20': 'chr20', '21': 'chr21', '22': 'chr22',
 '3': 'chr3', '4': 'chr4', '5': 'chr5', '6': 'chr6', '7': 'chr7', '8': 'chr8', '9': 'chr9', 'X': 'chrX', 'Y': 'chrY',
 'MT': 'chrM', 'NW_009646201.1': 'chr1'}


def download_file(url, to_dir=tempfile.gettempdir(), verify=True):
    if url.startswith('gs://'):
        url = f'https://storage.googleapis.com/{requests.utils.quote(url[5:])}'

    if not (url and url.startswith(("http://", "https://"))):
        raise ValueError("Invalid url: {}".format(url))

    local_file_path = os.path.join(to_dir, os.path.basename(url.rstrip('/')))

    is_gz = url.endswith(".gz") or url.endswith(".zip")
    response = requests.get(url, stream=is_gz, verify=True if verify==None else verify)
    input_iter = response if is_gz else response.iter_content()

    logger.info("Downloading {} to {}".format(url, local_file_path))
    input_iter = tqdm.tqdm(input_iter, unit=" data" if is_gz else " lines")

    with open(local_file_path, 'wb') as f:
        f.writelines(input_iter)

    input_iter.close()

    return local_file_path


def convert_json2tsv(json_path):
    with open(json_path, 'r') as f:
        data = json.load(f)
    tsv_path = f'{json_path[:-5]}.tsv' if json_path.endswith('.json') else f'{json_path}.tsv'
    with open(tsv_path, 'w') as f:
        header = '\t'.join(data[0].keys())
        f.write(header + '\n')
        for row in data:
            f.write('\t'.join([str(v) for v in row.values()]) + '\n')
    return tsv_path


def unzip_file(path):
    if path.endswith('.zip'):
        unzip_file = path.split('/')[-1][:-4]
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extract(unzip_file, path=os.path.dirname(path))
    return path[:-4]


def load_ht(config, force_write=True):
    logger.info(f'Downloading dataset from {config["input_path"]}.')
    dn_path = download_file(config['input_path'], verify=config.get('verify_ssl'))

    logger.info(f'Loading hail table from {dn_path}.')
    types = config['field_types'] if config.get('field_types') else {}
    if config['input_type'] == 'vcf':
        ht = hl.import_vcf(dn_path, force_bgz=True, contig_recoding=contig_recoding).rows()
    elif config['input_type'] == 'json':
        tsv_path = convert_json2tsv(dn_path)
        ht = hl.import_table(tsv_path, types=types)
    else:
        ht = hl.import_table(dn_path, types=types)

    if config.get('annotate'):
        ht = ht.annotate(**{field: func(ht) for field, func in config['annotate'].items()})

    ht = ht.filter(ht.locus.contig == 'chrM')

    ht = ht.key_by('locus', 'alleles')

    logger.info(f'Writing hail table to {config["output_path"]}.')
    ht.write(config['output_path'], overwrite=force_write)
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
