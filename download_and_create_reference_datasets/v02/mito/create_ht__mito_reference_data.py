#!/usr/bin/env python3

# Download/load mitochondrial reference datasets, annotate required fields into a hail table,
# and save it to a file.

import argparse
import logging
import json
import tempfile
import os

from datetime import datetime
from functools import reduce
import urllib
import hail as hl

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()

contig_recoding={'1': 'chr1', '10': 'chr10', '11': 'chr11', '12': 'chr12', '13': 'chr13', '14': 'chr14', '15': 'chr15',
 '16': 'chr16', '17': 'chr17', '18': 'chr18', '19': 'chr19', '2': 'chr2', '20': 'chr20', '21': 'chr21', '22': 'chr22',
 '3': 'chr3', '4': 'chr4', '5': 'chr5', '6': 'chr6', '7': 'chr7', '8': 'chr8', '9': 'chr9', 'X': 'chrX', 'Y': 'chrY',
 'MT': 'chrM', 'NW_009646201.1': 'chr1'}

CONFIG = {
    'gnomad': {
        'path': 'gs://gcp-public-data--gnomad/release/3.1/ht/genomes/gnomad.genomes.v3.1.sites.chrM.ht',
        'input_type': 'ht',
        'select': ['AN', 'AC_hom', 'AC_het', 'AF_hom', 'AF_het', 'max_hl']
    },
    'mitomap': {
        'path': 'gs://seqr-reference-data/GRCh38/MITOMAP/Mitomap Confirmed Mutations Feb. 04 2022.tsv',
        'input_type': 'tsv',
        'annotate': {
            'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.Allele.first_match_in('m.([0-9]+)')[0])),
            'alleles': lambda ht: ht.Allele.first_match_in('m.[0-9]+([ATGC]+)>([ATGC]+)'),
            'pathogenic': lambda ht: hl.is_defined(ht['Associated Diseases'])
        },
        'select': ['pathogenic']
    },
    'mitimpact': {
        'path': 'gs://seqr-reference-data/GRCh38/MitImpact/MitImpact_db_3.0.7.txt',  # from https://mitimpact.css-mendel.it/cdn/MitImpact_db_3.0.7.txt.zip',
        'input_type': 'tsv',
        'annotate': {
            'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.Start)),
            'alleles': lambda ht: [ht.Ref, ht.Alt],
            'APOGEE_score': lambda ht: hl.parse_float(ht.APOGEE_score),
        },
        'select': ['APOGEE_score']
    },
    'hmtvar': {
        'path': 'https://storage.googleapis.com/seqr-reference-data/GRCh38/HmtVar/HmtVar%20Jan.%2010%202022.json',  # from https://www.hmtvar.uniba.it/api/main/',
        'input_type': 'json',
        'annotate': {
            'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.nt_start)),
            'alleles': lambda ht: [ht.ref_rCRS, ht.alt],
            'disease_score': lambda ht: hl.parse_float(ht.disease_score),
        },
        'select': ['disease_score']
    },
    'helix': {
        'path': 'gs://seqr-reference-data/GRCh38/Hilex/HelixMTdb_20200327.tsv',  # from https://helix-research-public.s3.amazonaws.com/mito/HelixMTdb_20200327.tsv',
        'input_type': 'tsv',
        'field_types': {'counts_hom': hl.tint32, 'AF_hom': hl.tfloat64, 'counts_het': hl.tint32,
                        'AF_het': hl.tfloat64, 'max_ARF': hl.tfloat64, 'alleles': hl.tarray(hl.tstr)},
        'annotate': {
            'locus': lambda ht: hl.locus('chrM', hl.parse_int32(ht.locus.split(':')[1])),
        },
        'select': ['counts_hom', 'AF_hom', 'counts_het', 'AF_het', 'max_ARF']
    },
    'clinvar': {
        'path': 'gs://seqr-reference-data/GRCh38/clinvar/clinvar.GRCh38.2022-01-10.vcf.gz',  # from ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
        'input_type': 'vcf',
        'annotate': {
            'ALLELEID': lambda ht: ht.info.ALLELEID,
            'CLNSIG': lambda ht: ht.info.CLNSIG,
            'CLNREVSTAT': lambda ht: ht.info.CLNREVSTAT,
        },
        'select': ['ALLELEID', 'CLNSIG', 'CLNREVSTAT']
    },
    'dbnsfp': {
        'path': 'gs://seqr-reference-data/GRCh38/all_reference_data/v2/combined_reference_data_grch38-2.0.4.ht',
        'input_type': 'ht',
        'select': ['dbnsfp']
    }
}


def convert_json(json_path):
    f, josn_fname = tempfile.mkstemp(suffix='.json', text=True)
    os.close(f)
    urllib.request.urlretrieve(json_path, josn_fname)
    with open(josn_fname, 'r') as f:
        data = json.load(f)
    f, tsv_fname = tempfile.mkstemp(suffix='.tsv', text=True)
    os.close(f)
    with open(tsv_fname, 'w') as f:
        header = '\t'.join(data[0].keys())
        f.write(header + '\n')
        for row in data:
            f.write('\t'.join([str(v) for v in row.values()]) + '\n')
    os.remove(josn_fname)
    return tsv_fname


def load_hts(datasets):
    hts = []
    tsv_fname = None
    for dataset in datasets:
        logger.info(f'Loading dataset {dataset}.')
        types = CONFIG[dataset]['field_types'] if 'field_types' in CONFIG[dataset].keys() else {}
        if CONFIG[dataset]['input_type'] == 'ht':
            ht = hl.read_table(CONFIG[dataset]['path'])
        elif CONFIG[dataset]['input_type'] == 'tsv':
            ht = hl.import_table(CONFIG[dataset]['path'], types=types)
        elif CONFIG[dataset]['input_type'] == 'vcf':
            ht = hl.import_vcf(CONFIG[dataset]['path'], force_bgz=True, contig_recoding=contig_recoding).rows()
        elif CONFIG[dataset]['input_type'] == 'json':
            tsv_fname = convert_json(CONFIG[dataset]['path'])
            ht = hl.import_table(tsv_fname, types=types)

        if 'annotate' in CONFIG[dataset].keys():
            ht = ht.annotate(**{field: func(ht) for field, func in CONFIG[dataset]['annotate'].items()})

        ht = ht.filter(ht.locus.contig == 'chrM')

        ht = ht.annotate(**{dataset: hl.struct(**{field: ht[field] for field in CONFIG[dataset]['select']})})

        ht = ht.key_by('locus', 'alleles')
        ht = ht.select(dataset)
        hts.append(ht)
    return hts, tsv_fname


def join_hts(datasets):
    # Get a list of hail tables and combine into an outer join.
    hts, tsv_fname = load_hts(datasets)
    joined_ht = reduce((lambda joined_ht, ht: joined_ht.join(ht, 'outer')), hts)

    # Track the dataset we've added as well as the source path.
    included_dataset = {k: v['path'] for k, v in CONFIG.items() if k in datasets}
    # Add metadata, but also removes previous globals.
    joined_ht = joined_ht.select_globals(date=datetime.now().isoformat(),
                                         datasets=hl.dict(included_dataset))
    logger.info(joined_ht.describe(str))
    return joined_ht, tsv_fname


def run(args):
    hl.init(default_reference='GRCh38', min_block_size=128,master='local[32]')
    datasets = args.dataset.split(',')
    not_supported_dataset = [d for d in datasets if d not in CONFIG.keys()]
    if len(not_supported_dataset) > 0:
        logger.error(f'{len(not_supported_dataset)} datasets are not supported: {not_supported_dataset}')
        return

    logger.info(f'Loading and combining {datasets}')
    joined_ht, tsv_fname = join_hts(datasets)

    logger.info(f'Writing to {args.output_path}')
    joined_ht.write(args.output_path, overwrite=args.force_write)
    if tsv_fname:
        os.remove(tsv_fname)
    logger.info('Done')


if __name__ == "__main__":

    datasets = ','.join(CONFIG.keys())
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset', help=f'Reference dataset list, separated with commas, e.g. {datasets}', required=True)
    parser.add_argument('-o', '--output-path', help='Path and file name for the combined reference dataset', required=True)
    parser.add_argument('-f', '--force-write', help='Force write to an existing output file', action='store_true')
    args = parser.parse_args()

    run(args)
