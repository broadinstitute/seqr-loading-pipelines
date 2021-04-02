#!/usr/bin/env python3

import os
import logging
import vcf
import time
from elasticsearch import helpers as es_helpers
from tqdm import tqdm

from hail_scripts.shared.elasticsearch_client_v7 import ElasticsearchClient
from hail_scripts.shared.elasticsearch_utils import ELASTICSEARCH_INDEX

from genome_sv_pipeline.mapping_gene_ids import load_gencode
from sv_pipeline.load_data import get_sample_subset, get_sample_remap, get_es_index_name, get_es_schema,\
    CHROM_FIELD, SC_FIELD, SF_FIELD, GENES_FIELD, VARIANT_ID_FIELD, CALL_FIELD, START_COL, \
    END_COL, CN_FIELD, SAMPLE_ID_FIELD, GENOTYPES_FIELD, TRANSCRIPTS_FIELD, CHROM_TO_XPOS_OFFSET

CHR_ATTR = 'CHROM'
AC_ATTR = 'AC'
AF_ATTR = 'AF'
AN_ATTR = 'AN'
VAR_NAME_ATTR = 'ID'
CALL_ATTR = 'ALT'
START_ATTR = 'POS'
END_ATTR = 'END'
INFO_ATTR = 'INFO'
FILTER_ATTR = 'FILTER'
N_HET_ATTR = 'N_HET'
N_HOMALT_ATTR = 'N_HOMALT'
GNOMAND_SVS_ID_ATTR = 'gnomAD_V2_SVID'
GNOMAND_SVS_AF_ATTR = 'gnomAD_V2_AF'
CPX_TYPE_ATTR = 'CPX_TYPE'
CPX_INTERVALS_ATTR = 'CPX_INTERVALS'
CHR2_ATTR = 'CHR2'
END2_ATTR = 'END2'
GQ_ATTR = 'GQ'
RD_CN_ATTR = 'RD_CN'
GT_ATTR = 'GT'

SN_FIELD = 'sn'
FILTER_FIELD = 'filters'
N_HET_FIELD = 'sv_callset_Hemi'
N_HOMALT_FIELD = 'sv_callset_Hom'
GNOMAD_SVS_ID_FIELD = 'gnomad_svs_ID'
GNOMAD_SVS_AF_FIELD = 'gnomad_svs_AF'
CPX_TYPE_FIELD = 'cpx_type'
CPX_INTERVALS_FIELD = 'cpx_intervals'
SV_DETAIL_FIELD = 'svDetail'
CHR2_FIELD = 'chr2'
END2_FIELD = 'end2'
GQ_FIELD = 'gq'
NUM_ALT_FIELD = 'num_alt'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COL_CONFIGS = {
    CHR_ATTR: {'field_name': CHROM_FIELD, 'format': lambda val: val.lstrip('chr')},
    AC_ATTR: {'root_attr': INFO_ATTR, 'field_name': SC_FIELD, 'format': lambda val: val[0]},
    AF_ATTR: {'root_attr': INFO_ATTR, 'field_name': SF_FIELD, 'format': lambda val: val[0]},
    AN_ATTR: {'root_attr': INFO_ATTR, 'field_name': SN_FIELD},
    VAR_NAME_ATTR: {'field_name': VARIANT_ID_FIELD},
    CALL_ATTR: {'field_name': CALL_FIELD, 'format': lambda val: val[0].type},
    CPX_TYPE_ATTR: {'root_attr': INFO_ATTR, 'field_name': CPX_TYPE_FIELD, 'allow_missing': True},
    CPX_INTERVALS_ATTR: {'root_attr': INFO_ATTR, 'field_name': CPX_INTERVALS_FIELD, 'allow_missing': True},
    START_ATTR: {'field_name': START_COL},
    END_ATTR: {'root_attr': INFO_ATTR, 'field_name': END_COL},
    FILTER_ATTR: {'field_name': FILTER_FIELD, 'allow_missing': True, 'format': lambda val: val.remove('PASS') if 'PASS' in val else val},
    N_HET_ATTR: {'root_attr': INFO_ATTR, 'field_name': N_HET_FIELD},
    N_HOMALT_ATTR: {'root_attr': INFO_ATTR, 'field_name': N_HOMALT_FIELD},
    GNOMAND_SVS_ID_ATTR: {'root_attr': INFO_ATTR, 'field_name': GNOMAD_SVS_ID_FIELD, 'allow_missing': True},
    GNOMAND_SVS_AF_ATTR: {'root_attr': INFO_ATTR, 'field_name': GNOMAD_SVS_AF_FIELD, 'allow_missing': True},
    CHR2_ATTR:  {'root_attr': INFO_ATTR, 'field_name': CHR2_FIELD, 'format': lambda val: val.lstrip('chr'), 'allow_missing': True},
    END2_ATTR: {'root_attr': INFO_ATTR, 'field_name': END2_FIELD, 'allow_missing': True},
    GQ_ATTR: {'root_attr': 'data', 'field_name': GQ_FIELD, 'allow_missing': True},
    RD_CN_ATTR: {'root_attr': 'data', 'field_name': CN_FIELD, 'allow_missing': True},
    GT_ATTR: {'root_attr': 'data', 'field_name': NUM_ALT_FIELD, 'allow_missing': True, 'format': lambda val: int(val[0])+int(val[2])},
}

CORE_COLUMNS = [CHR_ATTR, AC_ATTR, AF_ATTR, AN_ATTR, VAR_NAME_ATTR, CALL_ATTR, CPX_TYPE_ATTR, CPX_INTERVALS_ATTR, START_ATTR, END_ATTR,
                FILTER_ATTR, N_HET_ATTR, N_HOMALT_ATTR, GNOMAND_SVS_ID_ATTR, GNOMAND_SVS_AF_ATTR, CHR2_ATTR, END2_ATTR]
SAMPLE_COLUMNS = [GQ_ATTR, RD_CN_ATTR, GT_ATTR]

gene_id_mapping = {}


def get_field_val(row, col, format_kwargs=None):
    """
    Get the parsed output value of a field in the raw data

    :param row: list representing the raw input row
    :param col: string identifier for the column
    :param format_kwargs: optional arguments to pass to the value formatter
    :return: parsed value
    """
    root_attr = getattr(row, COL_CONFIGS[col].get('root_attr', ''), None)
    if root_attr:
        val = root_attr.get(col) if isinstance(root_attr, dict) else getattr(root_attr, col, None)
    else:
        val = getattr(row, col, None)
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


def parse_sorted_transcript_consequences(info):
    trans = []
    for col in info.keys():
        if col.startswith('PROTEIN_CODING_') and isinstance(info[col], list):
            trans += [{'gene_symbol': gene,
                       'gene_id': gene_id_mapping[gene],
                       'predicted_consequence': col.split('__')[-1]
                       } for gene in info[col]]
    return trans


def parse_sv_row(row, parsed_svs_by_id):
    """
    Parse the given row into the desired SV output format and add it to the dictionary of parsed SVs

    :param row: list representing the raw input row
    :param parsed_svs_by_id: dictionary of parsed SVs keyed by ID
    :param sample_id: the sample id for the row
    :return: none
    """
    samples = []
    for sample in row.samples:
        if not sample.is_variant:
            continue
        parsed_sample = get_parsed_column_values(sample, SAMPLE_COLUMNS)
        parsed_sample[SAMPLE_ID_FIELD] = sample.sample
        samples.append(parsed_sample)

    if not samples:  # no sample has this variant
        return False

    parsed_row = get_parsed_column_values(row, CORE_COLUMNS)
    variant_id = parsed_row.get(VARIANT_ID_FIELD)
    parsed_svs_by_id[variant_id] = parsed_row

    parsed_svs_by_id[variant_id][TRANSCRIPTS_FIELD] = parse_sorted_transcript_consequences(row.INFO)
    parsed_svs_by_id[variant_id][GENES_FIELD] = [trans['gene_symbol'] for trans in
                                                 parsed_svs_by_id[variant_id][TRANSCRIPTS_FIELD]
                                                 if trans['predicted_consequence'] != 'NEAREST_TSS']

    parsed_svs_by_id[variant_id][GENOTYPES_FIELD] = samples

    return True


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
        if not parse_sv_row(row, parsed_svs_by_name):
            skipped_svs.add(row.ID)
            return False
        return True

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


def parse_cpx_intervals(cpx_intervals):
    intervals = []
    for interval in cpx_intervals:
        types = interval.split('_chr')
        chrs = types[1].split(':')
        pos = chrs[1].split('-')
        intervals.append({'alt': types[0], 'chrom':chrs[0], 'start': int(pos[0]), 'end': int(pos[1])})
    return intervals


def format_sv(sv):
    """
    Post-processing to format SVs for export

    :param sv: parsed SV
    :return: none
    """
    if sv[CALL_FIELD].startswith('INS:'):
        sv['svTypeDetail'] = sv[CALL_FIELD].split(':', 1)[1]
        sv[CALL_FIELD] = 'INS'
    elif sv[CALL_FIELD] == 'CPX':
        sv['svTypeDetail'] = sv[CPX_TYPE_FIELD]
        sv['cpxIntervals'] = parse_cpx_intervals(sv[CPX_INTERVALS_FIELD])
    sv.pop(CPX_TYPE_FIELD)
    sv.pop(CPX_INTERVALS_FIELD)

    sv['transcriptConsequenceTerms'] = [sv[CALL_FIELD]]
    sv['pos'] = sv[START_COL]
    sv['xpos'] = CHROM_TO_XPOS_OFFSET[sv[CHROM_FIELD]] + sv[START_COL]
    sv['xstart'] = sv['xpos']
    sv['xstop'] = CHROM_TO_XPOS_OFFSET[sv[CHR2_FIELD]] + sv[END2_FIELD] if sv[END2_FIELD] else\
        CHROM_TO_XPOS_OFFSET[sv[CHROM_FIELD]] + sv[END_COL]
    sv.pop(END2_FIELD)
    sv.pop(CHR2_FIELD)

    sv['samples'] = []
    for genotype in sv[GENOTYPES_FIELD]:
        sample_id = genotype['sample_id']
        sv['samples'].append(sample_id)

        if genotype[CN_FIELD]:
            cn_key = 'samples_cn_{}'.format(genotype[CN_FIELD]) if genotype[CN_FIELD] < 4 else 'samples_cn_gte_4'
            if cn_key not in sv:
                sv[cn_key] = []
            sv[cn_key].append(sample_id)

        num_alt_key = 'samples_num_alt_{}'.format(genotype[NUM_ALT_FIELD])
        if num_alt_key not in sv:
            sv[num_alt_key] = []
        sv[num_alt_key].append(sample_id)


def export_to_elasticsearch(es_host, es_port, rows, index_name, meta, es_password, num_shards=6):
    """
    Export SV data to elasticsearch

    :param es_host: elasticsearch server host
    :param es_port: elasticsearch server port
    :param rows: parsed SV rows to export
    :param index_name: elasticsearch index name
    :param meta: index metadata
    :param num_shards: number of shards for the index
    :return: none
    """
    es_client = ElasticsearchClient(host=es_host, port=es_port, es_password=es_password)

    all_fields = {}
    nested_fields = {GENOTYPES_FIELD: {}, TRANSCRIPTS_FIELD: {}, 'cpxIntervals': {}}

    for row in rows:
        all_fields.update({k: v for k, v in row.items() if v})
        for col, val in nested_fields.items():
            if row.get(col):
                val.update(row[col][0])
    elasticsearch_schema = get_es_schema(all_fields, nested_fields)

    if es_client.es.indices.exists(index=index_name):
        logger.info('Deleting existing index')
        es_client.es.indices.delete(index=index_name)

    logger.info('Setting up index')
    es_client.create_index(index_name, elasticsearch_schema, num_shards=num_shards, _meta=meta)

    es_client.route_index_to_temp_es_cluster(index_name)

    es_actions = [{
        '_index': index_name,
        '_op_type': ELASTICSEARCH_INDEX,
        '_id': row[VARIANT_ID_FIELD],
        '_source': row,
    } for row in rows]

    logger.info('Starting bulk export')
    success_count, _ = es_helpers.bulk(es_client.es, es_actions, chunk_size=2000)
    logger.info('Successfully created {} records'.format(success_count))

    es_client.es.indices.forcemerge(index=index_name)

    es_client.route_index_off_temp_es_cluster(index_name)


def main():
    guid = 'R0332_cmg_estonia_wgs'
    input_dataset = 'vcf/sv.vcf.gz'
    sample_type = 'WGS'

    start_time = time.time()
    global gene_id_mapping
    gene_id_mapping = load_gencode(29, genome_version='38')

    mapping_time = time.time()
    print('Time for loading gene ID mapping table: {:.2f} seconds.'.format(mapping_time - start_time))

    sample_subset = get_sample_subset(guid, sample_type)
    sample_remap = get_sample_remap(guid, sample_type)
    message = 'Subsetting to {} samples'.format(len(sample_subset))
    if sample_remap:
        message += ' (remapping {} samples)'.format(len(sample_remap))
    logger.info(message)

    subset_samples_time = time.time()
    print('Time for subsetting samples: {:.2f} seconds.'.format(subset_samples_time - mapping_time))

    parsed_svs_by_name = subset_and_group_svs(
        input_dataset,
        sample_subset,
        sample_remap,
        sample_type,
        ignore_missing_samples=True
    )
    logger.info('Found {} SVs'.format(len(parsed_svs_by_name)))

    parse_svs_time = time.time()
    print('Time for parsing SVs: {:.2f} seconds.'.format(parse_svs_time - subset_samples_time))

    parsed_svs = parsed_svs_by_name.values()

    logger.info('\nFormatting for ES export')
    for sv in parsed_svs:
        format_sv(sv)

    a = [v['sortedTranscriptConsequences'] for v in parsed_svs if
         v['sortedTranscriptConsequences'] and [gene for gene in v['sortedTranscriptConsequences'] if
                                                gene['gene_id'] == 'Not Found']]
    gene_id_not_found = {g['gene_symbol'] for sub in a for g in sub if g['gene_id']=='Not Found'}
    logger.info('\nThere are {} genes which Ids not being mapped: {}'.format(len(gene_id_not_found), gene_id_not_found))

    format_svs_time = time.time()
    print('Time for formatting SVs: {:.2f} seconds.'.format(format_svs_time - parse_svs_time))

    meta = {
      'genomeVersion': '38',
      'sampleType': sample_type,
      'datasetType': 'SV',
      'sourceFilePath': input_dataset,
    }
    index_name = get_es_index_name(guid, meta)
    logger.info('Exporting {} docs to ES index {}'.format(len(parsed_svs), index_name))
    es_host = 'localhost'
    es_port = '9200'
    es_password = None
    num_shards = None
    export_to_elasticsearch(es_host, es_port, parsed_svs, index_name, meta, es_password, num_shards)

    export_es_time = time.time()
    print('Time for exporting to Elasticsearch: {:.2f} seconds.'.format(export_es_time - format_svs_time))
    print('Total time: {:.2f} minutes.'.format((export_es_time - start_time)/60))

    logger.info('DONE')


if __name__ == '__main__':
    main()

# test_data_parsing('R0332_cmg_estonia_wgs', 'vcf/sv.vcf.gz')
# Outputs:
# INFO:genome_sv_pipeline.mapping_gene_ids:Re-using /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29lift37.annotation.gtf.gz previously downloaded from http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_29/GRCh37_mapping/gencode.v29lift37.annotation.gtf.gz
# INFO:genome_sv_pipeline.mapping_gene_ids:Re-using /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29.annotation.gtf.gz previously downloaded from http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_29/gencode.v29.annotation.gtf.gz
# INFO:genome_sv_pipeline.mapping_gene_ids:Loading /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29lift37.annotation.gtf.gz (genome version: 37)
# 2753539 gencode records [00:11, 243009.31 gencode records/s]
# INFO:genome_sv_pipeline.mapping_gene_ids:Loading /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29.annotation.gtf.gz (genome version: 38)
# 2742022 gencode records [00:09, 293228.13 gencode records/s]
# Time for loading gene ID mapping table: 21.09 seconds.
# INFO:genome_sv_pipeline.mapping_gene_ids:Get 59227 gene id mapping records
# Time for subsetting samples: 3.24 seconds.
# INFO:__main__:Subsetting to 167 samples
# 145568 rows [14:29, 167.37 rows/s]
# INFO:__main__:Found 106 sample ids
# INFO:__main__:Missing the following 61 samples:
# E00859946, HK015_0036, HK015_0038_D2, HK017-0044, HK017-0045, HK017-0046, HK032_0081, HK032_0081_2_D2, HK035_0089, HK060-0154_1, HK060-0155_1, HK060-0156_1, HK061-0157_D1, HK061-0158_D1, HK061-0159_D1, HK079-001_D2, HK079-002_D2, HK079-003_D2, HK080-001_D2, HK080-002_D2, HK080-003_D2, HK081-001_D2, HK081-002_D2, HK081-003_D2, HK085-001_D2, HK085-002_D2, HK085-004_D2, HK085-006_D2, HK100-001_D1, HK100-002_D1, HK100-003_D1, HK100-004_D1, HK104-001_D2, HK104-002_D2, HK108-001_1, HK108-002_1, HK108-003_1, HK112-001_1, HK112-002_1, HK112-003_1, HK115-001_1, HK115-002_1, HK115-003_1, HK117-001_1, HK117-002_1, HK117-003_1, HK119-001_1, HK119-002_1, HK119-003_1, OUN_HK124_001_D1, OUN_HK124_002_D1, OUN_HK124_003_D1, OUN_HK126_001_D1, OUN_HK126_002_D1, OUN_HK126_003_D1, OUN_HK131_001_D1, OUN_HK131_002_D1, OUN_HK131_003_D1, OUN_HK132_001_D1, OUN_HK132_002_D1, OUN_HK132_003_D1
# INFO:__main__:Found 67275 SVs
# INFO:__main__:
# Formatting for ES export
# Time for parsing SVs: 869.73 seconds.
# Time for formatting SVs: 2.28 seconds.
# INFO:__main__:
# There are 0 genes which Ids not being mapped: set()
# INFO:__main__:Exporting 67275 docs to ES index r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402
# INFO:elasticsearch:GET http://localhost:9200/ [status:200 request:0.010s]
# INFO:root:{'cluster_name': 'elasticsearch',
#  'cluster_uuid': 'f2eIQ6bCRM2axogPkrM7bA',
#  'name': 'c35606e34bf6',
#  'tagline': 'You Know, for Search',
#  'version': {'build_date': '2020-07-21T16:40:44.668009Z',
#              'build_flavor': 'default',
#              'build_hash': 'b5ca9c58fb664ca8bf9e4057fc229b3396bf3a89',
#              'build_snapshot': False,
#              'build_type': 'docker',
#              'lucene_version': '8.5.1',
#              'minimum_index_compatibility_version': '6.0.0-beta1',
#              'minimum_wire_compatibility_version': '6.8.0',
#              'number': '7.8.1'}}
# INFO:elasticsearch:HEAD http://localhost:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402 [status:200 request:0.007s]
# INFO:__main__:Deleting existing index
# INFO:elasticsearch:DELETE http://localhost:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402 [status:200 request:0.183s]
# INFO:__main__:Setting up index
# INFO:root:==> index _meta: {'datasetType': 'SV',
#  'genomeVersion': '38',
#  'sampleType': 'WGS',
#  'sourceFilePath': 'vcf/sv.vcf.gz'}
# INFO:root:create_mapping - elasticsearch schema:
# {'contig': {'type': 'keyword'},
#  'cpxIntervals': {'properties': {'alt': {'type': 'keyword'},
#                                  'chrom': {'type': 'keyword'},
#                                  'end': {'type': 'integer'},
#                                  'start': {'type': 'integer'}},
#                   'type': 'nested'},
#  'detailType': {'type': 'keyword'},
#  'end': {'type': 'integer'},
#  'filters': {'type': 'keyword'},
#  'geneIds': {'type': 'keyword'},
#  'genotypes': {'properties': {'cn': {'type': 'integer'},
#                               'gq': {'type': 'integer'},
#                               'num_alt': {'type': 'integer'},
#                               'sample_id': {'type': 'keyword'}},
#                'type': 'nested'},
#  'gnomad_svs_AF': {'type': 'double'},
#  'gnomad_svs_ID': {'type': 'keyword'},
#  'pos': {'type': 'integer'},
#  'samples': {'type': 'keyword'},
#  'samples_cn_1': {'type': 'keyword'},
#  'samples_cn_2': {'type': 'keyword'},
#  'samples_cn_3': {'type': 'keyword'},
#  'samples_cn_gte_4': {'type': 'keyword'},
#  'samples_num_alt_1': {'type': 'keyword'},
#  'samples_num_alt_2': {'type': 'keyword'},
#  'sc': {'type': 'integer'},
#  'sf': {'type': 'double'},
#  'sn': {'type': 'integer'},
#  'sortedTranscriptConsequences': {'properties': {'gene_id': {'type': 'keyword'},
#                                                  'gene_symbol': {'type': 'keyword'},
#                                                  'predicted_consequence': {'type': 'keyword'}},
#                                   'type': 'nested'},
#  'start': {'type': 'integer'},
#  'svType': {'type': 'keyword'},
#  'sv_callset_Hemi': {'type': 'integer'},
#  'sv_callset_Hom': {'type': 'integer'},
#  'transcriptConsequenceTerms': {'type': 'keyword'},
#  'variantId': {'type': 'keyword'},
#  'xpos': {'type': 'long'},
#  'xstart': {'type': 'long'},
#  'xstop': {'type': 'long'}}
# INFO:root:==> creating elasticsearch index r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402
# INFO:elasticsearch:PUT http://localhost:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402 [status:200 request:0.574s]
# INFO:root:==> Setting r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402 settings = {'index.routing.allocation.require._name': 'elasticsearch-es-data-loading*', 'index.routing.allocation.exclude._name': ''}
# INFO:elasticsearch:PUT http://localhost:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402/_settings [status:200 request:0.147s]
# INFO:__main__:Starting bulk export
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.874s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.981s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.974s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.915s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.939s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.928s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.920s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.996s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.982s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:1.164s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.956s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.892s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:1.032s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:1.009s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:1.017s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.973s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.973s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:1.048s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.995s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:1.068s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.985s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.895s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.937s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.951s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:1.046s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.830s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.936s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.867s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.833s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.978s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.897s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.970s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.940s]
# INFO:elasticsearch:POST http://localhost:9200/_bulk [status:200 request:0.564s]
# INFO:__main__:Successfully created 67275 records
# INFO:elasticsearch:POST http://localhost:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402/_forcemerge [status:200 request:8.685s]
# INFO:root:==> Setting r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402 settings = {'index.routing.allocation.require._name': '', 'index.routing.allocation.exclude._name': 'elasticsearch-es-data-loading*'}
# INFO:elasticsearch:PUT http://localhost:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402/_settings [status:200 request:0.179s]
# INFO:__main__:DONE
# Time for exporting to Elasticsearch: 46.13 seconds.
