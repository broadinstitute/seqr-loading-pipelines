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


def _download_file(url, to_dir=tempfile.gettempdir(), skip_verify=False):
    if not (url and url.startswith(("http://", "https://"))):
        raise ValueError("Invalid url: {}".format(url))

    local_file_path = os.path.join(to_dir, os.path.basename(url.rstrip('/')))

    if not skip_verify:
        response = requests.head(url)
        size = int(response.headers.get('Content-Length', '0'))
        if os.path.isfile(local_file_path) and os.path.getsize(local_file_path) == size:
            logger.info("Re-using {} previously downloaded from {}".format(local_file_path, url))
            return local_file_path

    is_gz = url.endswith(".gz") or url.endswith(".zip")
    response = requests.get(url, stream=is_gz, verify=not skip_verify)
    input_iter = response if is_gz else response.iter_content()

    logger.info("Downloading {} to {}".format(url, local_file_path))
    input_iter = tqdm.tqdm(input_iter, unit=" data" if is_gz else " lines")

    with open(local_file_path, 'wb') as f:
        f.writelines(input_iter)

    input_iter.close()

    return local_file_path


def _convert_json_to_tsv(json_path):
    with open(json_path, 'r') as f:
        data = json.load(f)
    tsv_path = f'{json_path[:-5]}.tsv' if json_path.endswith('.json') else f'{json_path}.tsv'
    with open(tsv_path, 'w') as f:
        header = '\t'.join(data[0].keys())
        f.write(header + '\n')
        for row in data:
            f.write('\t'.join([str(v) for v in row.values()]) + '\n')
    return tsv_path


def _load_mito_ht(config, force_write=True):
    logger.info(f'Downloading dataset from {config["input_path"]}.')
    dn_path = _download_file(config['input_path'], skip_verify=config.get('skip_verify_ssl'))

    if dn_path.endswith('.zip'):
        with zipfile.ZipFile(dn_path, 'r') as zip:
            zip.extractall(path=os.path.dirname(dn_path))
        dn_path = dn_path[:-4]

    logger.info(f'Loading hail table from {dn_path}.')
    types = config['field_types'] if config.get('field_types') else {}
    if config['input_type'] == 'json':
        tsv_path = _convert_json_to_tsv(dn_path)
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

    hl.init(default_reference='GRCh38')

    _load_mito_ht(config, args.force_write)
