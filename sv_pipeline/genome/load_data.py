import argparse
import os
import time
import hail as hl
import logging

from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient

from sv_pipeline.utils.common import get_sample_subset, get_sample_remap, get_es_index_name, CHROM_TO_XPOS_OFFSET
from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode, get_gene_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WGS_SAMPLE_TYPE = 'WGS'
BASIC_FIELDS = {
        'contig': lambda rows: rows.locus.contig.replace('^chr', ''),
        'sc': lambda rows: rows.info.AC[0],
        'sf': lambda rows: rows.info.AF[0],
        'sn': lambda rows: rows.info.AN,
        'svType': lambda rows: hl.if_else(rows.alleles[1].startswith('<INS:'), 'INS', rows.alleles[1].replace('[<>]', '')),
        'start': lambda rows: rows.locus.position,
        'end': lambda rows: rows.info.END,
        'sv_callset_Hemi': lambda rows: rows.info.N_HET,
        'sv_callset_Hom': lambda rows: rows.info.N_HOMALT,
        'gnomad_svs_ID': lambda rows: rows.info.gnomAD_V2_SVID,
        'gnomad_svs_AF': lambda rows: rows.info.gnomAD_V2_AF,
        'pos': lambda rows: rows.locus.position,
        'filters': lambda rows: hl.array(rows.filters.filter(lambda x: x != 'PASS')),
        'xpos': lambda rows: hl.literal(CHROM_TO_XPOS_OFFSET).get(rows.locus.contig.replace('^chr', '')) + rows.locus.position,
    }

INTERVAL_TYPE = 'array<struct{type: str, chrom: str, start: int32, end: int32}>'
COMPUTED_FIELDS = {
    'xstart': lambda rows: rows.xpos,
    'xstop': lambda rows: hl.if_else(hl.is_defined(rows.info.END2),
                                     hl.literal(CHROM_TO_XPOS_OFFSET).get(rows.info.CHR2.replace('^chr', '')) + rows.info.END2,
                                     rows.xpos),
    'transcriptConsequenceTerms': lambda rows: [rows.svType],
    'svTypeDetail': lambda rows: hl.if_else(rows.svType == 'CPX', rows.info.CPX_TYPE,
                                            hl.if_else(rows.alleles[1].startswith('<INS:'), rows.alleles[1][5:-1],
                                                       hl.null('str'))),
    'cpxIntervals': lambda rows: hl.if_else(hl.is_defined(rows.info.CPX_INTERVALS),
                                            rows.info.CPX_INTERVALS.map(
                                                lambda x: hl.struct(type=x.split('_chr')[0],
                                                                    chrom=x.split('_chr')[1].split(':')[0],
                                                                    start=hl.int32(x.split(':')[1].split('-')[0]),
                                                                    end=hl.int32(x.split('-')[1]))),
                                            hl.null(hl.dtype(INTERVAL_TYPE))),
    'sortedTranscriptConsequences': lambda rows: rows.gene_affected.flatmap(
        lambda x: x[0].map(lambda y: hl.struct(gene_symbol=y, gene_id=get_gene_id(y), predicted_consequence=x[1]))),
    'geneIds': lambda rows: rows.gene_affected.filter(lambda x: x[1] != 'NEAREST_TSS').flatmap(lambda x: x[0]),
    'samples_num_alt_0': lambda rows: get_sample_num_alt_x(rows, 0),
    'samples_num_alt_1': lambda rows: get_sample_num_alt_x(rows, 1),
    'samples_num_alt_2': lambda rows: get_sample_num_alt_x(rows, 2),
}
FIELDS = list(BASIC_FIELDS.keys()) + list(COMPUTED_FIELDS.keys()) + ['genotypes']
STR_ARRAY_FIELDS = {'filters', 'geneIds', 'samples_num_alt_0', 'samples_num_alt_1', 'samples_num_alt_2'}


def get_sample_num_alt_x(rows, n):
    return rows.genotypes.filter(lambda x: x.num_alt == n).map(lambda x: x.sample_id)


def sub_setting_mt(project_guid, mt, sample_type, skip_sample_subset, ignore_missing_samples):
    found_samples = {col.s for col in mt.key_cols_by().cols().collect()}
    if skip_sample_subset:
        sample_subset = found_samples
    else:
        sample_subset = get_sample_subset(project_guid, sample_type)
        if len(found_samples) != len(sample_subset):
            missed_samples = sample_subset - found_samples
            missing_sample_error = 'Missing the following {} samples:\n{}'.format(
                len(missed_samples), ', '.join(sorted(missed_samples))
            )
            if ignore_missing_samples:
                logger.info(missing_sample_error)
            else:
                skipped_samples = found_samples - sample_subset
                logger.info('Samples in callset but skipped:\n{}'.format(', '.join(sorted(skipped_samples))))
                raise Exception(missing_sample_error)
        sample_remap = get_sample_remap(project_guid, sample_type)
        message = 'Subsetting to {} samples'.format(len(sample_subset))
        if sample_remap:
            message += ' (remapping {} samples)'.format(len(sample_remap))
            sample_subset = sample_subset - set(sample_remap.keys()) + set(sample_remap.values())
            mt = mt.annotate_cols(**sample_remap)
        logger.info(message)

    mt = mt.filter_cols(hl.literal(sample_subset).contains(mt['s']))

    genotypes = hl.agg.collect(hl.struct(sample_id=mt.s, gq=mt.GQ, num_alt=mt.GT.n_alt_alleles(), cn=mt.RD_CN))
    mt = mt.annotate_rows(genotypes=genotypes)
    return mt.filter_rows(mt.genotypes.any(lambda x: x.num_alt > 0)).rows()


def annotate_fields(rows):
    rows = rows.annotate(**{k: v(rows) for k, v in BASIC_FIELDS.items()})

    rows = rows.annotate(gene_affected = hl.filter(lambda x: hl.is_defined(x[0]),
                              [(rows.info[col], col.split('__')[-1]) for col in
                               [gene_col for gene_col in rows.info if gene_col.startswith('PROTEIN_CODING__')
                                and rows.info[gene_col].dtype == hl.dtype('array<str>')]]))

    rows = rows.annotate(**{k: v(rows) for k, v in COMPUTED_FIELDS.items()})

    # replace empty string array fields with nulls
    array_fields = {field: hl.if_else(hl.len(rows[field]) > 0, rows[field], hl.null(hl.dtype('array<str>')))
                    for field in STR_ARRAY_FIELDS}
    rows = rows.annotate(**array_fields)

    rows = rows.rename({'rsid': 'variantId'})

    return rows.key_by('variantId').select(*FIELDS)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('input_dataset', help='input VCF file')
    p.add_argument('--matrixtable-path', default='', help='path for Hail MatrixTable data')
    p.add_argument('--skip-sample-subset', action='store_true')
    p.add_argument('--ignore-missing-samples', action='store_true')
    p.add_argument('--project-guid', required=True, help='the guid of the target seqr project')
    p.add_argument('--gencode-release', default=29)
    p.add_argument('--gencode-path', default='', help='path for downloaded Gencode data')
    p.add_argument('--es-host', default='localhost')
    p.add_argument('--es-port', default='9200')
    p.add_argument('--num-shards', default=6)
    p.add_argument('--block-size', default=2000)

    args = p.parse_args()

    if args.matrixtable_path == '':
        args.matrixtable_path = '{}.mt'.format(os.path.splitext(args.input_dataset)[0])

    hl.init()

    load_gencode(args.gencode_release, genome_version=WGS_SAMPLE_TYPE, download_path=args.gencode_path)

    # For the CMG dataset, we need to do hl.import_vcf() for once for all projects.
    if os.path.isdir(args.matrixtable_path):
        reminder = 'If the input VCF file has been changed, or you just want to import VCF again, please delete the MatrixTable.'
        logger.info('Use the existing MatrixTable at {}. {}'.format(args.matrixtable_path, reminder))
    else:
        hl.import_vcf(args.input_dataset, reference_genome='GRCh38').write(args.matrixtable_path)
        logger.info('The VCF file has been imported to the MatrixTable at {}.'.format(args.matrixtable_path))

    mt = hl.read_matrix_table(args.matrixtable_path)

    start_time = time.time()
    rows = sub_setting_mt(args.project_guid, mt, WGS_SAMPLE_TYPE, args.skip_sample_subset, args.ignore_missing_samples)
    logger.info('Variant counts: {}'.format(rows.count()))

    rows = annotate_fields(rows)

    meta = {
      'genomeVersion': '38',
      'sampleType': WGS_SAMPLE_TYPE,
      'datasetType': 'SV',
      'sourceFilePath': args.input_dataset,
    }
    index_name = get_es_index_name(args.project_guid, meta)

    es_password = os.environ.get('PIPELINE_ES_PASSWORD', '')
    es_client = ElasticsearchClient(host=args.es_host, port=args.es_port, es_password=es_password)

    es_client.export_table_to_elasticsearch(
        rows,
        index_name=index_name,
        index_type_name='_doc',
        block_size=args.block_size,
        num_shards=args.num_shards,
        delete_index_before_exporting=True,
        export_globals_to_index_meta=True,
        verbose=True,
    )
    logger.info('Total time for subsetting, annotating, and exporting: {}'.format(time.time() - start_time))


if __name__ == '__main__':
    main()
