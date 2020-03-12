#!/usr/bin/env python

import argparse
import re
from tqdm import tqdm

CHR_COL = 'chr'
START_COL = 'start'
END_COL = 'end'
QS_COL = 'QS'
CN_COL = 'CN'
CALL_COL = 'call'
SAMPLE_COL = 'sample'
NUM_EXON_COL = 'num_exon'
DEFRAGGED_COL = 'defragged'
SC_COL = 'sc'
SF_COL = 'sf'
VAR_NAME_COL = 'var_name'

SAMPLE_ID_FIELD = 'sample_id'
GENOTYPES_FIELD = 'genotypes'

BOOL_MAP = {'TRUE': True, 'FALSE': False}

COL_CONFIGS = {
    CHR_COL: {'field_name': 'contig', 'format': lambda val: val.lstrip('chr')},
    SC_COL: {'format': int},
    SF_COL: {'format': float},
    VAR_NAME_COL: {'field_name': 'variantId', 'format': lambda val, call='any': '{}_{}'.format(val, call)},
    CALL_COL: {'field_name': 'transcriptConsequenceTerms', 'format': lambda val: [val]},
    START_COL: {'format': int},
    END_COL: {'format': int},
    QS_COL: {'field_name': 'qs', 'format': int},
    CN_COL: {'field_name': 'cn', 'format': int},
    NUM_EXON_COL: {'format': int},
    DEFRAGGED_COL: {'format': lambda val: BOOL_MAP[val]},
    SAMPLE_COL: {
        'field_name': SAMPLE_ID_FIELD,
        'format': lambda val: re.search('(\d+)_(?P<sample_id>.+)_v\d_Exome_GCP', val).group('sample_id'),
    },
}

CORE_COLUMNS = [CHR_COL, SC_COL, SF_COL, CALL_COL]
SAMPLE_COLUMNS = [START_COL, END_COL, QS_COL, CN_COL,  NUM_EXON_COL, DEFRAGGED_COL, SAMPLE_COL]
COLUMNS = CORE_COLUMNS + SAMPLE_COLUMNS + [VAR_NAME_COL]


def get_sample_subset(sample_subset_file):
    with open(sample_subset_file, 'r') as f:
        header = f.readline()
        if header.strip() != 's':
            raise Exception('Missing header for sample subset file, expected "s" but found {}'.format(header))
        return {line.strip() for line in f}


def get_field_val(row, col, header_indices, format_kwargs=None):
    val = row[header_indices[col]]
    format_func = COL_CONFIGS[col].get('format')
    if format_func:
        val = format_func(val, **format_kwargs) if format_kwargs else format_func(val)
    return val


def get_parsed_column_values(row, header_indices, columns):
    return {COL_CONFIGS[col].get('field_name', col): get_field_val(row, col, header_indices) for col in columns}


def parse_sv_row(row, parsed_svs_by_id, header_indices):
    variant_id = get_field_val(row, VAR_NAME_COL, header_indices, format_kwargs={'call': row[header_indices[CALL_COL]]})
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


def subset_and_group_svs(input_dataset, sample_subset, ignore_missing_samples):
    parsed_svs_by_name = {}
    found_samples = set()
    skipped_samples = set()
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
            else:
                skipped_samples.add(sample_id)

    print('Found {} sample ids'.format(len(found_samples)))
    if len(found_samples) != len(sample_subset):
        missed_samples = sample_subset - found_samples
        missing_sample_error = 'Missing the following {} samples:\n{}'.format(
            len(missed_samples), ', '.join(sorted(missed_samples))
        )
        if ignore_missing_samples:
            print(missing_sample_error)
        else:
            missing_sample_error += '\nSkipped samples in callset:\n{}'.format(', '.join(sorted(skipped_samples)))
            raise Exception(missing_sample_error)

    return parsed_svs_by_name.values()


def load_data(input_dataset, sample_subset_file, ignore_missing_samples=False):
    sample_subset = get_sample_subset(sample_subset_file)
    print('Subsetting to {} samples'.format(len(sample_subset)))
    # TODO remap sample ids

    print('Parsing BED file')
    parsed_svs = subset_and_group_svs(input_dataset, sample_subset, ignore_missing_samples=ignore_missing_samples)
    print('Found {} SVs'.format(len(parsed_svs)))

    import json
    for sv in parsed_svs[:5]:
        print(json.dumps(sv, indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('input_dataset', help='input VCF or VDS')
    p.add_argument('--sample-subset')
    p.add_argument('--ignore-missing-samples', action='store_true')

    args = p.parse_args()

    load_data(args.input_dataset, args.sample_subset, ignore_missing_samples=args.ignore_missing_samples)

