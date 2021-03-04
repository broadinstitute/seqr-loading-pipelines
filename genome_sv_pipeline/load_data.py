#!/usr/bin/env python3

import logging
import subprocess
import vcf
from collections import defaultdict

from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GS_SAMPLE_PATH = 'gs://seqr-datasets/v02/GRCh38/RDG_{sample_type}_Broad_{internal}/base/projects/{project_guid}/{project_guid}_{file_ext}'


def _get_gs_samples(project_guid, file_ext, expected_header, sample_type, internal='Internal'):
    """
    Get sample metadata from files in google cloud

    :param project_guid: seqr project identifier
    :param file_ext: extension for the desired sample file
    :param expected_header: expected header to validate file
    :param sample_type: sample type (WES/WGS)
    :return: parsed data from the sample file as a list of lists
    """
    file = GS_SAMPLE_PATH.format(project_guid=project_guid, sample_type=sample_type, file_ext=file_ext, internal=internal)
    process = subprocess.Popen(
        'gsutil cat {}'.format(file), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    if process.wait() != 0:
        return None
    header = next(process.stdout).decode('utf-8')
    if header.strip() != expected_header:
        raise Exception('Missing header for sample file, expected "{}" but found {}'.format(
            expected_header, header))
    return [line.decode('utf-8').strip().split('\t') for line in process.stdout]


def get_sample_subset(project_guid, sample_type, **kwargs):
    """
    Get sample id subset for a given project

    :param project_guid: seqr project identifier
    :param sample_type: sample type (WES/WGS)
    :param internal: string 'Internal' or 'External'
    :return: set of sample ids
    """
    subset = _get_gs_samples(project_guid, file_ext='ids.txt', sample_type=sample_type, expected_header='s', **kwargs)
    if not subset:
        raise Exception('No sample subset file found')
    return {row[0] for row in subset}


def get_sample_remap(project_guid, sample_type):
    """
    Get an optional remapping for sample ids in the given project

    :param project_guid: seqr project identifier
    :param sample_type: sample type (WES/WGS)
    :return: dictionary mapping VCF sample ids to seqr sample ids, or None if no mapping available
    """
    remap = _get_gs_samples(project_guid, file_ext='remap.tsv', sample_type=sample_type, expected_header='s\tseqr_id')
    if remap:
        remap = {row[0]: row[1] for row in remap}
    return remap


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


def subset_rows_to_file(guid, **kwargs):
    print('\nguid', guid)
    sample_subset = get_sample_subset(guid, 'WGS', **kwargs)
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


def get_all_sample_rows(guid, **kwargs):
    print('\nguid', guid)
    sample_subset = get_sample_subset(guid, 'WGS', **kwargs)
    print('Project samples', len(sample_subset))
    vcf_reader = vcf.Reader(filename='vcf/sv.vcf.gz')
    samples = vcf_reader.samples
    missed_sample = [sample for sample in sample_subset if not sample in samples]
    print('Samples missed from the VCF data', len(missed_sample))
    sample_subset = [sample for sample in sample_subset if sample in samples]
    rows = load_vcf_data(vcf_reader, sample_subset)
    print(len(rows))


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

