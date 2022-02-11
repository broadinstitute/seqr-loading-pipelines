#!/usr/bin/env python3

# Download/load mitochondrial reference datasets from data sources, import them into hail tables, annotate fields if need,
# filter out non-mitochondrial variants, and save them to per source hail tables.

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

CONFIG = {
    'mitomap': {
        # The data source is a web page. So we download it manually and save them to the GCS.
        'input_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/MITOMAP/Mitomap Confirmed Mutations Feb. 04 2022.tsv',
        'input_type': 'tsv',
        'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/MITOMAP/Mitomap Confirmed Mutations Feb. 04 2022.ht',
        'annotate': {
            'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.Allele.first_match_in('m.([0-9]+)')[0])),
            'alleles': lambda ht: ht.Allele.first_match_in('m.[0-9]+([ATGC]+)>([ATGC]+)'),
            'pathogenic': lambda ht: hl.is_defined(ht['Associated Diseases'])
        },
    },
    'mitimpact': {
        'input_path': 'https://mitimpact.css-mendel.it/cdn/MitImpact_db_3.0.7.txt.zip',
        'input_type': 'tsv',
        'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/MitImpact/MitImpact_db_3.0.7.ht',
        'annotate': {
            'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.Start)),
            'alleles': lambda ht: [ht.Ref, ht.Alt],
            'APOGEE_score': lambda ht: hl.parse_float(ht.APOGEE_score),
        },
    },
    'hmtvar': {
        # The data source has certificate expiration issue. Manually downloaded from https://www.hmtvar.uniba.it/api/main/
        'input_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar%20Jan.%2010%202022.json',
        'input_type': 'json',
        'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar%20Jan.%2010%202022.ht',
        'annotate': {
            'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.nt_start)),
            'alleles': lambda ht: [ht.ref_rCRS, ht.alt],
            'disease_score': lambda ht: hl.parse_float(ht.disease_score),
        },
    },
    'helix': {
        'input_path': 'https://helix-research-public.s3.amazonaws.com/mito/HelixMTdb_20200327.tsv',
        'input_type': 'tsv',
        'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/Helix/HelixMTdb_20200327.ht',
        'field_types': {'counts_hom': hl.tint32, 'AF_hom': hl.tfloat64, 'counts_het': hl.tint32,
                        'AF_het': hl.tfloat64, 'max_ARF': hl.tfloat64, 'alleles': hl.tarray(hl.tstr)},
        'annotate': {
            'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.locus.split(':')[1])),
        },
    },
    'clinvar': {
        'input_path': 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
        'input_type': 'vcf',
        'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/clinvar/clinvar.GRCh38.chrM.ht',
        'annotate': {
            'ALLELEID': lambda ht: ht.info.ALLELEID,
            'CLNSIG': lambda ht: ht.info.CLNSIG,
            'CLNREVSTAT': lambda ht: ht.info.CLNREVSTAT,
        },
    },
    'dbnsfp': {
        'input_path': 'gs://seqr-reference-data/GRCh38/all_reference_data/v2/combined_reference_data_grch38-2.0.4.ht',
        'input_type': 'ht',
        'annotate': {
            'SIFT_pred': lambda ht: ht.dbnsfp.SIFT_pred,
            'Polyphen2_HVAR_pred': lambda ht: ht.dbnsfp.Polyphen2_HVAR_pred,
            'MutationTaster_pred': lambda ht: ht.dbnsfp.MutationTaster_pred,
            'FATHMM_pred': lambda ht: ht.dbnsfp.FATHMM_pred,
            'MetaSVM_pred': lambda ht: ht.dbnsfp.MetaSVM_pred,
            'REVEL_score': lambda ht: ht.dbnsfp.REVEL_score,
            'GERP_RS': lambda ht: ht.dbnsfp.GERP_RS,
            'phastCons100way_vertebrate': lambda ht: ht.dbnsfp.phastCons100way_vertebrate,
        },
        'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/dbnsfp/dbnsfp_GRCh38_chrM-2.0.4.ht',
    }
}


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
    if path.startswith('gs://') and not path.endswith('.ht'):
        path = path.replace('gs://', 'https://storage.googleapis.com/', 1)
    if path.startswith('http') or path.startswith('ftp'):
        filename = get_tempfile(suffix=path.split('/')[-1])
        urllib.request.urlretrieve(path, filename)
        if path.endswith('.zip'):
            unzip_file = path.split('/')[-1][:-4]
            unzip_path = os.path.dirname(filename)
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extract(unzip_file, path=unzip_path)
            os.remove(filename)
            filename = os.path.join(unzip_path, unzip_file)
    else:
        filename = path
    return filename


def load_hts(datasets, args):
    for dataset in datasets:
        tsv_fname = None

        logger.info(f'Downloading dataset {dataset}.')
        dn_fname = download_file(CONFIG[dataset]['input_path'])

        logger.info(f'Loading dataset {dataset} from {dn_fname}.')
        types = CONFIG[dataset]['field_types'] if CONFIG[dataset].get('field_types') else {}
        if CONFIG[dataset]['input_type'] == 'tsv':
            ht = hl.import_table(dn_fname, types=types)
        elif CONFIG[dataset]['input_type'] == 'vcf':
            ht = hl.import_vcf(dn_fname, force_bgz=True, contig_recoding=contig_recoding).rows()
        elif CONFIG[dataset]['input_type'] == 'json':
            tsv_fname = convert_json2tsv(dn_fname)
            ht = hl.import_table(tsv_fname, types=types)
        else:  # assume unspecified as an ht (hail table) type.
            ht = hl.read_table(dn_fname)

        if CONFIG[dataset].get('annotate'):
            ht = ht.annotate(**{field: func(ht) for field, func in CONFIG[dataset]['annotate'].items()})

        ht = ht.filter(ht.locus.contig == 'chrM')

        ht = ht.key_by('locus', 'alleles')

        logger.info(f'Writing dataset {dataset} to {CONFIG[dataset]["output_path"]}.')
        ht.write(CONFIG[dataset]['output_path'], overwrite=args.force_write)

        if tsv_fname:
            os.remove(tsv_fname)
        if dn_fname != CONFIG[dataset]['input_path']:
            os.remove(dn_fname)


def run(args):
    # If there are out-of-memory error, set the environment variable with the following command
    # $ export PYSPARK_SUBMIT_ARGS="--driver-memory 4G pyspark-shell"
    # "4G" in the environment variable can be bigger if your computer has a larger memory.
    hl.init(default_reference='GRCh38', min_block_size=128, master='local[32]')

    datasets = args.dataset.split(',')
    not_supported_dataset = [d for d in datasets if d not in CONFIG.keys()]
    if len(not_supported_dataset) > 0:
        logger.error(f'{len(not_supported_dataset)} datasets are not supported: {not_supported_dataset}')
        return

    logger.info(f'Loading and combining {datasets}')
    load_hts(datasets, args)

    logger.info('Done')


if __name__ == "__main__":

    datasets = ','.join(CONFIG.keys())
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset', help=f'Reference dataset list, separated with commas, e.g. {datasets}', required=True)
    parser.add_argument('-f', '--force-write', help='Force write to an existing output file', action='store_true')
    args = parser.parse_args()

    run(args)
