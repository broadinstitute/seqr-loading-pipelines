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
BATCH_COL = 'batch'
NUM_EXON_COL = 'num_exon'
DEFRAGGED_COL = 'defragged'
SC_COL = 'sc'
SF_COL = 'sf'
VAR_NAME_COL = 'var_name'

SAMPLES_COL = 'parsed_samples'

CORE_COLUMNS = [CHR_COL, SC_COL, SF_COL, VAR_NAME_COL, CALL_COL]
SAMPLE_COLUMNS = [START_COL, END_COL, QS_COL, CN_COL, BATCH_COL, NUM_EXON_COL, DEFRAGGED_COL]
COLUMNS = CORE_COLUMNS + SAMPLE_COLUMNS + [SAMPLE_COL]


def get_sample_subset(sample_subset_file):
    with open(sample_subset_file, 'r') as f:
        header = f.readline()
        if header.strip() != 's':
            raise Exception('Missing header for sample subset file, expected "s" but found {}'.format(header))
        return {line.strip() for line in f}


def get_seqr_sample_id(raw_sample_id):
    m = re.search('(\d+)_(?P<sample_id>.+)_v\d_Exome_GCP', raw_sample_id)
    return m.group('sample_id')


def parse_sv_row(row, sample_id, parsed_svs_by_name_call, header_indices):
    sv_key = '{}-{}'.format(row[header_indices[VAR_NAME_COL]], row[header_indices[CALL_COL]])
    sample_info = {col: row[header_indices[col]] for col in SAMPLE_COLUMNS}
    if sv_key in parsed_svs_by_name_call:
        existing_sv = parsed_svs_by_name_call[sv_key]
        existing_sv[SAMPLES_COL][sample_id] = sample_info
        # Use the largest coordinates for the merged SV
        existing_sv[START_COL] = min(existing_sv[START_COL], row[header_indices[START_COL]])
        existing_sv[END_COL] = max(existing_sv[END_COL], row[header_indices[END_COL]])
    else:
        parsed_row = {col: row[header_indices[col]] for col in CORE_COLUMNS + [START_COL, END_COL]}
        parsed_row[SAMPLES_COL] = {sample_id: sample_info}
        parsed_svs_by_name_call[sv_key] = parsed_row


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
            sample_id = get_seqr_sample_id(row[header_indices[SAMPLE_COL]])
            if sample_id in sample_subset:
                parse_sv_row(row, sample_id, parsed_svs_by_name, header_indices)
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
    
    cn_ranges = set()
    cns_with_variance = 0
    multi_sample_svs = 0
    for sv in parsed_svs:
        if len(sv[SAMPLES_COL]) > 1:
            multi_sample_svs += 1
            cns = {sam[CN_COL]for sam in sv[SAMPLES_COL].values()}
            if len(cns) > 1:
                cns_with_variance += 1
                cn_ranges.add(str(sorted(cns)))
    print('{} multi sample SVs'.format(multi_sample_svs))
    print('{} SVs with varying CN'.format(cns_with_variance))
    print(cn_ranges) # ['4', '5']", "['2', '3']", "['3', '4']", "['3', '5']", "['3', '4', '5']", "['2', '4']", "['0', '1']"]


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('input_dataset', help='input VCF or VDS')
    p.add_argument('--sample-subset')
    p.add_argument('--ignore-missing-samples', action='store_true')

    args = p.parse_args()

    load_data(args.input_dataset, args.sample_subset, ignore_missing_samples=args.ignore_missing_samples)

