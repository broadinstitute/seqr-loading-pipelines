#!/usr/bin/env python

import argparse
from datetime import datetime
from elasticsearch import helpers as es_helpers
import re
import subprocess
from tqdm import tqdm

from hail_scripts.shared.elasticsearch_client import ElasticsearchClient
from hail_scripts.shared.elasticsearch_utils import ELASTICSEARCH_INDEX

GS_SAMPLE_PATH = 'gs://seqr-datasets/v02/GRCh38/RDG_WES_Broad_Internal/base/projects/{project_guid}/{project_guid}_{file_ext}'

GENCODE_CHR_COL_IDX = 0
GENCODE_TYPE_COL_IDX = 2
GENCODE_START_COL_IDX = 3
GENCODE_END_COL_IDX = 4
GENCODE_INFO_COL_IDX = 8

CHR_COL = 'chr'
START_COL = 'start'
END_COL = 'end'
QS_COL = 'QS'
CN_COL = 'CN'
CALL_COL = 'svtype'
SAMPLE_COL = 'sample'
NUM_EXON_COL = 'genes_any_overlap_totalExons'
DEFRAGGED_COL = 'defragmented'
SC_COL = 'vac'
SF_COL = 'vaf'
VAR_NAME_COL = 'name'
GENES_COL = 'genes_any_overlap_Ensemble_ID'
IN_SILICO_COL = 'path'

CHROM_FIELD = 'contig'
SAMPLE_ID_FIELD = 'sample_id'
GENOTYPES_FIELD = 'genotypes'
CN_FIELD = 'cn'
QS_FIELD = 'qs'
GENES_FIELD = 'geneIds'
TRANSCRIPTS_FIELD = 'sortedTranscriptConsequences'
SF_FIELD = 'sf'
SC_FIELD = 'sc'
VARIANT_ID_FIELD = 'variantId'
CALL_FIELD = 'svType'
DEFRAGGED_FIELD = 'defragged'
NUM_EXON_FIELD = 'num_exon'

BOOL_MAP = {'TRUE': True, 'FALSE': False}

COL_CONFIGS = {
    CHR_COL: {'field_name': CHROM_FIELD, 'format': lambda val: val.lstrip('chr')},
    SC_COL: {'field_name': SC_FIELD, 'format': int},
    SF_COL: {'field_name': SF_FIELD, 'format': float},
    VAR_NAME_COL: {'field_name': VARIANT_ID_FIELD, 'format': lambda val, call='any': '{}_{}'.format(val, call)},
    CALL_COL: {'field_name': CALL_FIELD},
    START_COL: {'format': int},
    END_COL: {'format': int},
    QS_COL: {'field_name': QS_FIELD, 'format': int},
    CN_COL: {'field_name': CN_FIELD, 'format': int},
    NUM_EXON_COL: {'field_name': NUM_EXON_FIELD, 'format': lambda val: 0 if val == 'NA' else int(val)},
    DEFRAGGED_COL: {'field_name': DEFRAGGED_FIELD, 'format': lambda val: BOOL_MAP[val]},
    IN_SILICO_COL: {
        'field_name': 'StrVCTVRE_score',
        'format': lambda val: None if val == 'not_exonic' else float(val),
        'allow_missing': True,
    },
    SAMPLE_COL: {
        'field_name': SAMPLE_ID_FIELD,
        'format': lambda val: re.search('(\d+)_(?P<sample_id>.+)_v\d_Exome_GCP', val).group('sample_id'),
    },
    GENES_COL: {'field_name': GENES_FIELD, 'format': lambda genes: [gene.split('.')[0] for gene in genes.split(',')]},
}

CORE_COLUMNS = [CHR_COL, SC_COL, SF_COL, CALL_COL, GENES_COL]
SAMPLE_COLUMNS = [START_COL, END_COL, QS_COL, CN_COL,  NUM_EXON_COL, DEFRAGGED_COL, SAMPLE_COL]
COLUMNS = CORE_COLUMNS + SAMPLE_COLUMNS + [VAR_NAME_COL]

IN_SILICO_COLS = [VAR_NAME_COL, CALL_COL, IN_SILICO_COL]

QS_BIN_SIZE = 10

CHROMOSOMES = [
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21',
    '22', 'X', 'Y', 'M',
]
CHROM_TO_XPOS_OFFSET = {chrom: (1 + i)*int(1e9) for i, chrom in enumerate(CHROMOSOMES)}

ES_TYPE_MAP = {
    int: 'integer',
    float: 'double',
    str: 'keyword',
    bool: 'boolean',
}
ES_FIELD_TYPLE_MAP = {
    'xpos': 'long',
    'xstart': 'long',
    'xstop': 'long',
}
ES_INDEX_TYPE = 'structural_variant'
NUM_SHARDS = 6


def _get_gs_samples(project_guid, file_ext, expected_header):
    file = GS_SAMPLE_PATH.format(project_guid=project_guid, file_ext=file_ext)
    process = subprocess.Popen(
        'gsutil cat {}'.format(file), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    if process.wait() != 0:
        return None
    header = next(process.stdout)
    if header.strip() != expected_header:
        raise Exception('Missing header for sample file, expected "{}" but found {}'.format(
            expected_header, header))
    return [line.strip().split('\t') for line in process.stdout]


def get_sample_subset(project_guid):
    subset = _get_gs_samples(project_guid, file_ext='ids.txt', expected_header='s')
    if not subset:
        raise Exception('No sample subset file found')
    return {row[0] for row in subset}


def get_sample_remap(project_guid):
    remap = _get_gs_samples(project_guid, file_ext='remap.tsv', expected_header='s\tseqr_id')
    if remap:
        remap = {row[0]: row[1] for row in remap}
    return remap


def get_field_val(row, col, header_indices, format_kwargs=None):
    index = header_indices[col]
    if index > len(row):
        if COL_CONFIGS[col].get('allow_missing'):
            return None
        raise IndexError('Column "{}" is missing from row {}'.format(col, row))
    val = row[header_indices[col]]
    format_func = COL_CONFIGS[col].get('format')
    if format_func:
        val = format_func(val, **format_kwargs) if format_kwargs else format_func(val)
    return val


def get_variant_id(row, header_indices):
    return get_field_val(
        row, VAR_NAME_COL, header_indices, format_kwargs={'call': get_field_val(row, CALL_COL, header_indices)},
    )


def get_parsed_column_values(row, header_indices, columns):
    return {COL_CONFIGS[col].get('field_name', col): get_field_val(row, col, header_indices) for col in columns}


def parse_sv_row(row, parsed_svs_by_id, header_indices, sample_id):
    variant_id = get_variant_id(row, header_indices)
    if variant_id not in parsed_svs_by_id:
        parsed_svs_by_id[variant_id] = get_parsed_column_values(row, header_indices, CORE_COLUMNS)
        parsed_svs_by_id[variant_id][COL_CONFIGS[VAR_NAME_COL]['field_name']] = variant_id
        parsed_svs_by_id[variant_id][GENOTYPES_FIELD] = []

    sample_info = get_parsed_column_values(row, header_indices, SAMPLE_COLUMNS)
    sample_info[SAMPLE_ID_FIELD] = sample_id

    sv = parsed_svs_by_id[variant_id]
    sv[GENOTYPES_FIELD].append(sample_info)
    # Use the largest coordinates for the merged SV
    sv[START_COL] = min(sv.get(START_COL, float('inf')), sample_info[START_COL])
    sv[END_COL] = max(sv.get(END_COL, 0), sample_info[END_COL])
    sv[NUM_EXON_FIELD] = max(sv.get(NUM_EXON_FIELD, 0), sample_info[NUM_EXON_FIELD])


def load_file(file_path, parse_row, out_file_path=None, columns=None):
    out_file = None
    if out_file_path:
        out_file = open(out_file_path, 'w')

    with open(file_path, 'r') as f:
        header = f.readline()
        header_indices = {col: i for i, col in enumerate(header.split())}
        missing_cols = [col for col in columns or COLUMNS if col not in header_indices]
        if missing_cols:
            raise Exception('Missing expected columns: {}'.format(', '.join(missing_cols)))
        if out_file:
            out_file.write(header)

        for line in tqdm(f, unit=' rows'):
            row = line.split()
            parsed = parse_row(row, header_indices)
            if parsed and out_file:
                out_file.write(line)

    if out_file:
        out_file.close()


def subset_and_group_svs(input_dataset, sample_subset, sample_remap, ignore_missing_samples, write_subsetted_bed=False):
    parsed_svs_by_name = {}
    found_samples = set()
    skipped_samples = set()
    out_file_path = None
    if write_subsetted_bed:
        file_path = input_dataset.split('/')
        file_path[-1] = 'subset_{}'.format(file_path[-1])
        out_file_path ='/'.join(file_path)

    def _parse_row(row, header_indices):
        sample_id = get_field_val(row, SAMPLE_COL, header_indices)
        if sample_remap and sample_id in sample_remap:
            sample_id = sample_remap[sample_id]
        if sample_subset is None or sample_id in sample_subset:
            parse_sv_row(row, parsed_svs_by_name, header_indices, sample_id)
            found_samples.add(sample_id)
            return True
        else:
            skipped_samples.add(sample_id)
            return False

    load_file(input_dataset, _parse_row, out_file_path=out_file_path)

    print('Found {} sample ids'.format(len(found_samples)))
    if sample_subset:
        if len(found_samples) != len(sample_subset):
            missed_samples = sample_subset - found_samples
            missing_sample_error = 'Missing the following {} samples:\n{}'.format(
                len(missed_samples), ', '.join(sorted(missed_samples))
            )
            if ignore_missing_samples:
                print(missing_sample_error)
            else:
                print('Samples in callset:\n{}'.format(', '.join(sorted(skipped_samples))))
                raise Exception(missing_sample_error)

    return parsed_svs_by_name


def add_in_silico(svs_by_variant_id, file_path):
    def _parse_row(row, header_indices):
        variant_id = get_variant_id(row, header_indices)
        if variant_id in svs_by_variant_id:
            svs_by_variant_id[variant_id].update(get_parsed_column_values(row, header_indices, [IN_SILICO_COL]))

    load_file(file_path, _parse_row, columns=IN_SILICO_COLS)


def format_sv(sv):
    sv[TRANSCRIPTS_FIELD] = [{'gene_id': gene} for gene in sv[GENES_FIELD]]
    sv['transcriptConsequenceTerms'] = [sv[CALL_FIELD]]
    if sv[SF_FIELD]:
        sv['sn'] = int(sv[SC_FIELD] / sv[SF_FIELD])
    sv['pos'] = sv[START_COL]
    sv['xpos'] = CHROM_TO_XPOS_OFFSET[sv[CHROM_FIELD]] + sv[START_COL]
    sv['xstart'] = sv['xpos']
    sv['xstop'] = CHROM_TO_XPOS_OFFSET[sv[CHROM_FIELD]] + sv[END_COL]
    sv['samples'] = []
    for genotype in sv[GENOTYPES_FIELD]:
        sample_id = genotype['sample_id']
        sv['samples'].append(sample_id)

        cn_key = 'samples_cn_{}'.format(genotype['cn']) if genotype['cn'] < 4 else 'samples_cn_gte_4'
        if cn_key not in sv:
            sv[cn_key] = []
        sv[cn_key].append(sample_id)

        if genotype[QS_FIELD] > 1000:
            qs_key = 'samples_qs_gt_1000'
        else:
            qs_bin = genotype[QS_FIELD] / QS_BIN_SIZE
            qs_key = 'samples_qs_{}_to_{}'.format(qs_bin * 10, (qs_bin + 1) * 10)
        if qs_key not in sv:
            sv[qs_key] = []
        sv[qs_key].append(sample_id)

        if sv[START_COL] == genotype[START_COL] and sv[END_COL] == genotype[END_COL]:
            genotype.pop(START_COL)
            genotype.pop(END_COL)

        if sv[NUM_EXON_FIELD] == genotype[NUM_EXON_FIELD]:
            genotype.pop(NUM_EXON_FIELD)


def get_es_schema(all_fields, nested_fields):
    schema = {
        key: {'type': ES_FIELD_TYPLE_MAP.get(key) or ES_TYPE_MAP[type(val[0]) if isinstance(val, list) else type(val)]}
        for key, val in all_fields.items() if key not in nested_fields
    }
    for key, val_dict in nested_fields.items():
        schema[key] = {'type': 'nested', 'properties': get_es_schema(val_dict, {})}
    return schema


def get_es_index_name(project, meta):
    return '{project}__structural_variants__{sample_type}__grch{genome_version}__{datestamp}'.format(
        project=project,
        sample_type=meta['sampleType'],
        genome_version=meta['genomeVersion'],
        datestamp=datetime.today().strftime('%Y%m%d'),
    ).lower()


def export_to_elasticsearch(es_host, es_port, rows, index_name, meta):
    es_client = ElasticsearchClient(host=es_host, port=es_port)

    all_fields = {}
    nested_fields = {GENOTYPES_FIELD: {}, TRANSCRIPTS_FIELD: {}}
    for row in rows:
        all_fields.update({k: v for k, v in row.items() if v})
        for col, val in nested_fields.items():
            val.update(row[col][0])
    elasticsearch_schema = get_es_schema(all_fields, nested_fields)

    if es_client.es.indices.exists(index=index_name):
        print('Deleting existing index')
        es_client.es.indices.delete(index=index_name)

    print('Setting up index')
    es_client.create_or_update_mapping(
        index_name, ES_INDEX_TYPE, elasticsearch_schema, num_shards=NUM_SHARDS, _meta=meta
    )

    es_client.route_index_to_temp_es_cluster(index_name, True)

    es_actions = [{
        '_index': index_name,
        '_type': ES_INDEX_TYPE,
        '_op_type': ELASTICSEARCH_INDEX,
        '_id': row[VARIANT_ID_FIELD],
        '_source': row,
    } for row in rows]

    print('Starting bulk export')
    success_count, _ = es_helpers.bulk(es_client.es, es_actions, chunk_size=1000)
    print('Successfully created {} records'.format(success_count))

    es_client.es.indices.forcemerge(index=index_name)

    es_client.route_index_to_temp_es_cluster(index_name, False)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('input_dataset', help='input VCF or VDS')
    p.add_argument('--skip-sample-subset', action='store_true')
    p.add_argument('--write-subsetted-bed', action='store_true')
    p.add_argument('--ignore-missing-samples', action='store_true')
    p.add_argument('--in-silico')
    p.add_argument('--project-guid')
    p.add_argument('--es-host', default='localhost')
    p.add_argument('--es-port', default='9200')

    args = p.parse_args()

    sample_subset = None
    sample_remap = None
    if not args.skip_sample_subset:
        sample_subset = get_sample_subset(args.project_guid)
        sample_remap = get_sample_remap(args.project_guid)
        message = 'Subsetting to {} samples'.format(len(sample_subset))
        if sample_remap:
            message += ' (remapping {} samples)'.format(len(sample_remap))
        print(message)

    print('Parsing BED file')
    parsed_svs_by_name = subset_and_group_svs(
        args.input_dataset,
        sample_subset,
        sample_remap,
        ignore_missing_samples=args.ignore_missing_samples,
        write_subsetted_bed=args.write_subsetted_bed
    )
    print('Found {} SVs'.format(len(parsed_svs_by_name)))

    print('Adding in silico predictors')
    add_in_silico(parsed_svs_by_name, args.in_silico)

    parsed_svs = parsed_svs_by_name.values()

    print('\nFormatting for ES export')
    for sv in tqdm(parsed_svs, unit=' sv records'):
        format_sv(sv)

    meta = {
      'genomeVersion': '38',
      'sampleType': 'WES',
      'datasetType': 'SV',
      'sourceFilePath': args.input_dataset,
    }
    index_name = get_es_index_name(args.project_guid, meta)
    print('Exporting {} docs to ES index {}'.format(len(parsed_svs), index_name))
    export_to_elasticsearch(args.es_host, args.es_port, parsed_svs, index_name, meta)

    print('DONE')
