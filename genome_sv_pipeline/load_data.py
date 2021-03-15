#!/usr/bin/env python3

import os
import logging
import vcf
from collections import defaultdict

from tqdm import tqdm

from sv_pipeline.load_data import get_sample_subset, get_sample_remap,\
    CHROM_FIELD, SC_FIELD, SF_FIELD, VARIANT_ID_FIELD, CALL_FIELD, START_COL, \
    END_COL, QS_FIELD, CN_FIELD, SAMPLE_ID_FIELD, GENOTYPES_FIELD

CHR_ATTR = 'CHROM'
AC_ATTR = 'AC'
AF_ATTR = 'AF'
AN_ATTR = 'AN'
VAR_NAME_ATTR = 'ID'
CALL_ATTR = 'ALT'
START_ATTR = 'POS'
END_ATTR = 'END'
QS_ATTR = 'RD_GQ'
CN_ATTR = 'RD_CN'
INFO_ATTR = 'INFO'

SN_FIELD = 'sn'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COL_CONFIGS = {
    CHR_ATTR: {'field_name': CHROM_FIELD, 'format': lambda val: val.lstrip('chr')},
    AC_ATTR: {'root_attr': INFO_ATTR, 'field_name': SC_FIELD},
    AF_ATTR: {'root_attr': INFO_ATTR, 'field_name': SF_FIELD},
    AN_ATTR: {'root_attr': INFO_ATTR, 'field_name': SN_FIELD},
    VAR_NAME_ATTR: {'field_name': VARIANT_ID_FIELD},
    CALL_ATTR: {'field_name': CALL_FIELD, 'format': lambda val: val[0]},
    START_ATTR: {'field_name': START_COL, 'format': int},
    END_ATTR: {'root_attr': INFO_ATTR, 'field_name': END_COL, 'format': int},
    QS_ATTR: {'root_attr': 'data', 'field_name': QS_FIELD, 'format': int, 'allow_missing': True},
    CN_ATTR: {'root_attr': 'data', 'field_name': CN_FIELD, 'format': int, 'allow_missing': True},
}

CORE_COLUMNS = [CHR_ATTR, AC_ATTR, AF_ATTR, AN_ATTR, VAR_NAME_ATTR, CALL_ATTR, START_ATTR, END_ATTR]
SAMPLE_COLUMNS = [QS_ATTR, CN_ATTR]


def get_field_val(row, col, format_kwargs=None):
    """
    Get the parsed output value of a field in the raw data

    :param row: list representing the raw input row
    :param col: string identifier for the column
    :param format_kwargs: optional arguments to pass to the value formatter
    :return: parsed value
    """
    root_attr = COL_CONFIGS[col].get('root_attr', col)
    val = getattr(row, root_attr)
    if val is None:
        if COL_CONFIGS[col].get('allow_missing'):
            return None
        raise IndexError('Column "{}" is missing from row {}'.format(col, row))

    if COL_CONFIGS[col].get('root_attr'):
        val = val.get(col) if isinstance(val, dict) else getattr(val, col)
    if val is None:
        if COL_CONFIGS[col].get('allow_missing'):
            return None
        raise IndexError('Column "{}" is missing from row {}'.format(col, row))

    format_func = COL_CONFIGS[col].get('format')
    if format_func:
        val = format_func(val, **format_kwargs) if format_kwargs else format_func(val)
    return val


def get_parsed_column_values(row, columns):
    """
    Get the parsed values from a given row for a given set of columns

    :param row: list representing the raw input row
    :param columns: list of string identifiers for the desired columns
    :return: dictionary representation of a parsed row
    """
    return {COL_CONFIGS[col].get('field_name', col): get_field_val(row, col) for col in columns}


def parse_sv_row(row, parsed_svs_by_id):
    """
    Parse the given row into the desired SV output format and add it to the dictionary of parsed SVs

    :param row: list representing the raw input row
    :param parsed_svs_by_id: dictionary of parsed SVs keyed by ID
    :param sample_id: the sample id for the row
    :return: none
    """
    parsed_row = get_parsed_column_values(row, CORE_COLUMNS)
    variant_id = parsed_row.get(VARIANT_ID_FIELD)
    parsed_svs_by_id[variant_id] = parsed_row

    samples = []
    for sample in row.samples:
        parsed_sample = get_parsed_column_values(sample, SAMPLE_COLUMNS)
        parsed_sample[SAMPLE_ID_FIELD] = sample.sample
        samples.append(parsed_sample)

    parsed_svs_by_id[variant_id][GENOTYPES_FIELD] = samples


def load_file(file_path, parse_row, sample_subset, out_file_path=None):
    """
    Validate and parse the given file using the given parse functionality

    :param file_path: path to the file for parsing
    :param parse_row: function to run on each row in the file, returns a boolean indicator if parsing was successful
    :param out_file_path: optional path to a file to write out the raw rows that were successfully parsed
    :param columns: expected columns in the input file
    :return: none
    """
    f = vcf.Reader(filename=file_path)
    if f:
        found_samples = {sample for sample in sample_subset if sample in f.samples}
        out_file = None
        if out_file_path:
            out_file = vcf.Writer(open(out_file_path, 'w'), f)

        for row in tqdm(f, unit=' rows'):
            parsed = parse_row(row)
            if parsed and out_file:
                out_file.write_record(row)

        if out_file:
            out_file.close()

        return found_samples


def subset_and_group_svs(input_dataset, sample_subset, sample_remap, sample_type, ignore_missing_samples, write_subsetted_vcf=False):
    """
    Parses raw SV calls from the input file into the desired SV output format for samples in the given subset

    :param input_dataset: file path for the raw SV calls
    :param sample_subset: optional list of samples to subset to
    :param sample_remap: optional mapping of raw sample ids to seqr sample ids
    :param sample_type: sample type (WES/WGS)
    :param ignore_missing_samples: whether or not to fail if samples in the subset have no raw data
    :param write_subsetted_vcf: whether or not to write a bed file with only the subsetted samples
    :return: dictionary of parsed SVs keyed by ID
    """
    parsed_svs_by_name = {}
    skipped_svs = set()
    out_file_path = None
    if write_subsetted_vcf:
        file_name = 'subset_{}'.format(os.path.basename(input_dataset))
        out_file_path = os.path.join(os.path.dirname(input_dataset), file_name)

    def _parse_row(row):
        samples = [sample for sample in row.samples if sample.sample in sample_subset]
        row.samples = samples
        for sample in row.samples:
            if sample.gt_alleles[0] != '0' or sample.gt_alleles[1] != '0':
                parse_sv_row(row, parsed_svs_by_name)
                return row
        skipped_svs.add(row.ID)
        return None

    found_samples = load_file(input_dataset, _parse_row, sample_subset, out_file_path=out_file_path)

    logger.info('Found {} sample ids'.format(len(found_samples)))
    if sample_subset:
        if len(found_samples) != len(sample_subset):
            missed_samples = sample_subset - found_samples
            missing_sample_error = 'Missing the following {} samples:\n{}'.format(
                len(missed_samples), ', '.join(sorted(missed_samples))
            )
            if ignore_missing_samples:
                logger.info(missing_sample_error)
            else:
                logger.info('Samples in callset but skipped:\n{}'.format(', '.join(sorted(missed_samples))))
                raise Exception(missing_sample_error)

    return parsed_svs_by_name

def test_data_parsing(guid, input_dataset, sample_type='WGS'):
    sample_subset = get_sample_subset(guid, sample_type)
    sample_remap = get_sample_remap(guid, sample_type)
    message = 'Subsetting to {} samples'.format(len(sample_subset))
    if sample_remap:
        message += ' (remapping {} samples)'.format(len(sample_remap))
    logger.info(message)

    parsed_svs_by_name = subset_and_group_svs(
        input_dataset,
        sample_subset,
        sample_remap,
        sample_type,
        ignore_missing_samples=True
    )
    logger.info('Found {} SVs'.format(len(parsed_svs_by_name)))


def stat_sv_type():
    vcf_reader = vcf.Reader(filename='vcf/sv.vcf.gz')
    stat = defaultdict(int)
    for row in tqdm(vcf_reader, unit=' rows'):
        if len(row.ALT) != 1:
            print("Warning: Multiple ALTs.", row)
        stat[row.ALT[0].type] += 1
    print(stat)


def subset_vcf_rows_to_file(reader, writer, sample_subset):
    if not sample_subset:
        return 0
    cnt = 0
    for row in tqdm(reader, unit=' rows'):
        samples = [sample for sample in row.samples if sample.sample in sample_subset]
        row.samples = samples
        for sample in row.samples:
            if sample.gt_alleles[0] != '0' or sample.gt_alleles[1] != '0':
                writer.write_record(row)
                cnt += 1
                break
    return cnt


def subset_rows_to_file(guid):
    print('\nguid', guid)
    sample_subset = get_sample_subset(guid, 'WGS')
    print('Project samples', len(sample_subset))
    vcf_reader = vcf.Reader(filename='vcf/sv.vcf.gz')
    vcf_writer = vcf.Writer(open('vcf/sv.subset.{}.vcf'.format(guid), 'w'), vcf_reader)
    samples = vcf_reader.samples
    missed_sample = [sample for sample in sample_subset if not sample in samples]
    print('Samples missed from the VCF data', len(missed_sample))
    sample_subset = [sample for sample in sample_subset if sample in samples]
    cnt = subset_vcf_rows_to_file(vcf_reader, vcf_writer, sample_subset)
    print(cnt)


def load_vcf_data(vcf_reader, sample_subset):
    if not sample_subset:
        return []
    rows = []
    for row in tqdm(vcf_reader, unit=' rows'):
        samples = [sample for sample in row.samples if sample.sample in sample_subset]
        row.samples = samples
        for sample in row.samples:
            if sample.gt_alleles[0] != '0' or sample.gt_alleles[1] != '0':
                rows.append(row)
                break
    return rows


def get_all_sample_rows(guid):
    print('\nguid', guid)
    sample_subset = get_sample_subset(guid, 'WGS')
    print('Project samples', len(sample_subset))
    vcf_reader = vcf.Reader(filename='vcf/sv.vcf.gz')
    samples = vcf_reader.samples
    missed_sample = [sample for sample in sample_subset if not sample in samples]
    print('Samples missed from the VCF data', len(missed_sample))
    sample_subset = [sample for sample in sample_subset if sample in samples]
    rows = load_vcf_data(vcf_reader, sample_subset)
    print(len(rows))


test_data_parsing('R0332_cmg_estonia_wgs', 'vcf/sv.vcf.gz')
# Outputs:
# INFO:__main__:Subsetting to 167 samples
# 145568 rows [33:30, 72.39 rows/s]
# INFO:__main__:Found 106 sample ids
# INFO:__main__:Missing the following 61 samples:
# E00859946, HK015_0036, HK015_0038_D2, HK017-0044, HK017-0045, HK017-0046, HK032_0081, HK032_0081_2_D2, HK035_0089, HK060-0154_1, HK060-0155_1, HK060-0156_1, HK061-0157_D1, HK061-0158_D1, HK061-0159_D1, HK079-001_D2, HK079-002_D2, HK079-003_D2, HK080-001_D2, HK080-002_D2, HK080-003_D2, HK081-001_D2, HK081-002_D2, HK081-003_D2, HK085-001_D2, HK085-002_D2, HK085-004_D2, HK085-006_D2, HK100-001_D1, HK100-002_D1, HK100-003_D1, HK100-004_D1, HK104-001_D2, HK104-002_D2, HK108-001_1, HK108-002_1, HK108-003_1, HK112-001_1, HK112-002_1, HK112-003_1, HK115-001_1, HK115-002_1, HK115-003_1, HK117-001_1, HK117-002_1, HK117-003_1, HK119-001_1, HK119-002_1, HK119-003_1, OUN_HK124_001_D1, OUN_HK124_002_D1, OUN_HK124_003_D1, OUN_HK126_001_D1, OUN_HK126_002_D1, OUN_HK126_003_D1, OUN_HK131_001_D1, OUN_HK131_002_D1, OUN_HK131_003_D1, OUN_HK132_001_D1, OUN_HK132_002_D1, OUN_HK132_003_D1
# INFO:__main__:Found 67889 SVs

#stat_sv_type()
# Outputs:
# 145568 rows [13:32, 179.23 rows/s]
# defaultdict(<class 'int'>, {'DUP': 39546, 'BND': 12837, 'DEL': 71236, 'CNV': 526, 'INS': 20540, 'CPX': 768, 'INV': 111, 'CTX': 4})


#subset_rows_to_file('R0332_cmg_estonia_wgs')
# Outputs:
# guid R0332_cmg_estonia_wgs
# Project samples 167
# Samples missed from the VCF data 61
# 145568 rows [15:14, 159.12 rows/s]
# 67889

#get_all_sample_rows('R0332_cmg_estonia_wgs')
# Outputs:
# guid R0332_cmg_estonia_wgs
# Project samples 167
# Samples missed from the VCF data 61
# 145568 rows [14:54, 162.82 rows/s]
# 67889


#subset_rows_to_file('R0485_cmg_beggs_wgs')
# Outputs:
# guid R0485_cmg_beggs_wgs
# Project samples 47
# Samples missed from the VCF data 19
# 145568 rows [12:36, 192.50 rows/s]
# 35729

#get_all_sample_rows('R0485_cmg_beggs_wgs')
# Outputs:
# guid R0485_cmg_beggs_wgs
# Project samples 47
# Samples missed from the VCF data 19
# 145568 rows [13:32, 179.12 rows/s]
# 35729


#subset_rows_to_file('R0487_cmg_myoseq_wgs')
# Outputs:
# guid R0487_cmg_myoseq_wgs
# Project samples 11
# Samples missed from the VCF data 2
# 145568 rows [12:36, 192.44 rows/s]
# 59432

#get_all_sample_rows('R0487_cmg_myoseq_wgs')
# Outputs:
# guid R0487_cmg_myoseq_wgs
# Project samples 11
# Samples missed from the VCF data 2
# 145568 rows [13:09, 184.32 rows/s]
# 59432

