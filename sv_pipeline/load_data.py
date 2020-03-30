#!/usr/bin/env python

import argparse
from datetime import datetime
from elasticsearch import helpers as es_helpers
import re
from tqdm import tqdm

from hail_scripts.shared.elasticsearch_client import ElasticsearchClient
from hail_scripts.shared.elasticsearch_utils import ELASTICSEARCH_INDEX

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
NUM_EXON_COL = 'num_exon'
DEFRAGGED_COL = 'defragged'
SC_COL = 'sc'
SF_COL = 'sf'
VAR_NAME_COL = 'var_name'
IN_SILICO_COL = 'path'

CHROM_FIELD = 'contig'
SAMPLE_ID_FIELD = 'sample_id'
GENOTYPES_FIELD = 'genotypes'
CN_FIELD = 'cn'
QS_FIELD = 'qs'
GENES_FIELD = 'geneIds'
TRANSCRIPTS_FIELD = 'sortedTranscriptConsequences'
VARIANT_ID_FIELD = 'variantId'
CALL_FIELD = 'svType'

BOOL_MAP = {'TRUE': True, 'FALSE': False}

COL_CONFIGS = {
    CHR_COL: {'field_name': CHROM_FIELD, 'format': lambda val: val.lstrip('chr')},
    SC_COL: {'format': int},
    SF_COL: {'format': float},
    VAR_NAME_COL: {'field_name': VARIANT_ID_FIELD, 'format': lambda val, call='any': '{}_{}'.format(val, call)},
    CALL_COL: {'field_name': CALL_FIELD},
    START_COL: {'format': int},
    END_COL: {'format': int},
    QS_COL: {'field_name': QS_FIELD, 'format': int},
    CN_COL: {'field_name': CN_FIELD, 'format': int},
    NUM_EXON_COL: {'format': int},
    DEFRAGGED_COL: {'format': lambda val: BOOL_MAP[val]},
    IN_SILICO_COL: {'field_name': 'StrVCTVRE_score', 'format': float, 'allow_missing': True},
    SAMPLE_COL: {
        'field_name': SAMPLE_ID_FIELD,
        'format': lambda val: re.search('(\d+)_(?P<sample_id>.+)_v\d_Exome_GCP', val).group('sample_id'),
    },
}

CORE_COLUMNS = [CHR_COL, SC_COL, SF_COL, CALL_COL, IN_SILICO_COL]
SAMPLE_COLUMNS = [START_COL, END_COL, QS_COL, CN_COL,  NUM_EXON_COL, DEFRAGGED_COL, SAMPLE_COL]
COLUMNS = CORE_COLUMNS + SAMPLE_COLUMNS + [VAR_NAME_COL]

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


def get_sample_subset(sample_subset_file):
    with open(sample_subset_file, 'r') as f:
      
        header = f.readline()
        if header.strip() != 's':
            raise Exception('Missing header for sample subset file, expected "s" but found {}'.format(header))
        return {line.strip() for line in f}


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


def get_parsed_column_values(row, header_indices, columns):
    return {COL_CONFIGS[col].get('field_name', col): get_field_val(row, col, header_indices) for col in columns}


def parse_sv_row(row, parsed_svs_by_id, header_indices):
    variant_id = get_field_val(
        row, VAR_NAME_COL, header_indices, format_kwargs={'call': get_field_val(row, CALL_COL, header_indices)},
    )
    if variant_id not in parsed_svs_by_id:
        parsed_svs_by_id[variant_id] = get_parsed_column_values(row, header_indices, CORE_COLUMNS)
        parsed_svs_by_id[variant_id][COL_CONFIGS[VAR_NAME_COL]['field_name']] = variant_id
        parsed_svs_by_id[variant_id][GENOTYPES_FIELD] = []

    sample_info = get_parsed_column_values(row, header_indices, SAMPLE_COLUMNS)

    sv = parsed_svs_by_id[variant_id]
    sv[GENOTYPES_FIELD].append(sample_info)
    # Use the largest coordinates for the merged SV
    sv[START_COL] = min(sv.get(START_COL, float('inf')), sample_info[START_COL])
    sv[END_COL] = max(sv.get(END_COL, 0), sample_info[END_COL])
    sv[NUM_EXON_COL] = max(sv.get(NUM_EXON_COL, 0), sample_info[NUM_EXON_COL])


def subset_and_group_svs(input_dataset, sample_subset, ignore_missing_samples, write_subsetted_bed=False):
    parsed_svs_by_name = {}
    found_samples = set()
    skipped_samples = set()
    out_file = None
    if write_subsetted_bed:
        file_path = input_dataset.split('/')
        file_path[-1] = 'subset_{}'.format(file_path[-1])
        out_file = open('/'.join(file_path), 'w')
    with open(input_dataset, 'r') as f:
        header_indices = {col: i for i, col in enumerate(f.readline().split())}
        missing_cols = [col for col in COLUMNS if col not in header_indices]
        if missing_cols:
            raise Exception('Missing expected columns: {}'.format(', '.join(missing_cols)))

        for line in tqdm(f, unit=' rows'):
            row = line.split()
            sample_id = get_field_val(row, SAMPLE_COL, header_indices)
            if sample_id in sample_subset:
                parse_sv_row(row, parsed_svs_by_name, header_indices)
                found_samples.add(sample_id)
                if out_file:
                    out_file.write(line)
            else:
                skipped_samples.add(sample_id)
    if out_file:
        out_file.close()

    print('Found {} sample ids'.format(len(found_samples)))
    if len(found_samples) != len(sample_subset):
        missed_samples = sample_subset - found_samples
        missing_sample_error = 'Missing the following {} samples:\n{}'.format(
            len(missed_samples), ', '.join(sorted(missed_samples))
        )
        if ignore_missing_samples:
            print(missing_sample_error)
        else:
            missing_sample_error += '\nSamples in callset:\n{}'.format(', '.join(sorted(skipped_samples)))
            raise Exception(missing_sample_error)

    return parsed_svs_by_name.values()


def add_transcripts(svs, gencode_file_path):
    svs.sort(key=lambda sv: (
        int(sv[CHROM_FIELD]) if sv[CHROM_FIELD].isdigit() else sv[CHROM_FIELD], sv[END_COL], sv[START_COL]
    ))
    for sv in svs:
        sv[TRANSCRIPTS_FIELD] = []
    current_sv_idx = 0
    new_chrom = None

    with open(gencode_file_path) as gencode_file:
        for i, line in enumerate(tqdm(gencode_file, unit=' gencode records')):
            line = line.rstrip('\r\n')
            if not line or line.startswith('#'):
                continue
            fields = line.split('\t')

            if fields[GENCODE_TYPE_COL_IDX] != 'transcript':
                continue

            chrom = fields[GENCODE_CHR_COL_IDX].lstrip('chr')
            start = int(fields[GENCODE_START_COL_IDX])
            end = int(fields[GENCODE_END_COL_IDX])
            transcript_info = None

            if new_chrom:
                if chrom != new_chrom:
                    continue
                else:
                    new_chrom = None

            while chrom != svs[current_sv_idx][CHROM_FIELD]:
                current_sv_idx += 1
                if current_sv_idx == len(svs):
                    # all SVs have been annotated
                    return

            while svs[current_sv_idx][END_COL] < start:
                current_sv_idx += 1
                if current_sv_idx == len(svs):
                    # all SVs have been annotated
                    return
                if svs[current_sv_idx][CHROM_FIELD] != chrom:
                    new_chrom = svs[current_sv_idx][CHROM_FIELD]
                    break

            if new_chrom:
                continue

            for sv in svs[current_sv_idx:]:
                if sv[CHROM_FIELD] != chrom or sv[START_COL] > end:
                    break
                if not transcript_info:
                    # Info field in the format "gene_id "ENSG00000223972.5"; transcript_id "ENST00000450305.2"; gene_type ..."
                    info_fields = {
                        x.strip().split()[0]: x.strip().split()[1].strip('"')
                        for x in fields[GENCODE_INFO_COL_IDX].split(';') if x != ''
                    }
                    transcript_info = {
                        'gene_id': info_fields['gene_id'].split('.')[0],
                        'transcript_id': info_fields['transcript_id'].split('.')[0],
                        'biotype': info_fields['gene_type'],
                    }

                sv[TRANSCRIPTS_FIELD].append(transcript_info)


def format_sv(sv):
    sv[GENES_FIELD] = list({transcript['gene_id'] for transcript in sv[TRANSCRIPTS_FIELD]})
    sv['transcriptConsequenceTerms'] = [sv[CALL_FIELD]]
    sv['sn'] = int(sv[SC_COL] / sv[SF_COL])
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

        if sv[NUM_EXON_COL] == genotype[NUM_EXON_COL]:
            genotype.pop(NUM_EXON_COL)


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
        all_fields.update(row)
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
    p.add_argument('--sample-subset')  # TODO get automatically from google cloud
    p.add_argument('--write-subsetted-bed', action='store_true')
    p.add_argument('--gencode')
    p.add_argument('--ignore-missing-samples', action='store_true')
    p.add_argument('--project-guid')
    p.add_argument('--es-host', default='localhost')
    p.add_argument('--es-port', default='9200')

    args = p.parse_args()

    sample_subset = get_sample_subset(args.sample_subset)
    print('Subsetting to {} samples'.format(len(sample_subset)))
    # TODO remap sample ids

    print('Parsing BED file')
    parsed_svs = subset_and_group_svs(
        args.input_dataset,
        sample_subset,
        ignore_missing_samples=args.ignore_missing_samples,
        write_subsetted_bed= args.write_subsetted_bed
    )
    print('Found {} SVs'.format(len(parsed_svs)))

    print('Adding gene annotations')
    add_transcripts(parsed_svs, args.gencode)

    print('\nFormatting for ES export')
    for sv in tqdm(parsed_svs, unit=' sv records'):
        format_sv(sv)

    meta = {
      'gencodeVersion': '33',  # TODO get from file path
      'genomeVersion': '38',
      'sampleType': 'WES',
      'datasetType': 'SV',
      'sourceFilePath': args.input_dataset,
    }
    index_name = get_es_index_name(args.project_guid, meta)
    print('Exporting {} docs to ES index {}'.format(len(parsed_svs), index_name))
    export_to_elasticsearch(args.es_host, args.es_port, parsed_svs, index_name, meta)

    print('DONE')
