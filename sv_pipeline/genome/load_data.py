import argparse
import hail as hl
import logging
import os
import time

from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient

from sv_pipeline.utils.common import get_sample_subset, get_sample_remap, get_es_index_name, CHROM_TO_XPOS_OFFSET
from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EXP_CHROM_TO_XPOS_OFFSET = hl.literal(CHROM_TO_XPOS_OFFSET)

WGS_SAMPLE_TYPE = 'WGS'

INTERVAL_TYPE = 'array<struct{type: str, chrom: str, start: int32, end: int32}>'

BASIC_FIELDS = {
    'contig': lambda rows: rows.locus.contig.replace('^chr', ''),
    'sc': lambda rows: rows.info.AC[0],
    'sf': lambda rows: rows.info.AF[0],
    'sn': lambda rows: rows.info.AN,
    'start': lambda rows: rows.locus.position,
    'end': lambda rows: hl.if_else(hl.is_defined(rows.info.END2), rows.info.END2, rows.info.END),
    'sv_callset_Het': lambda rows: rows.info.N_HET,
    'sv_callset_Hom': lambda rows: rows.info.N_HOMALT,
    'gnomad_svs_ID': lambda rows: rows.info.gnomAD_V2_SVID,
    'gnomad_svs_AF': lambda rows: rows.info.gnomAD_V2_AF,
    'pos': lambda rows: rows.locus.position,
    'filters': lambda rows: hl.array(rows.filters.filter(lambda x: x != 'PASS')),
    'xpos': lambda rows: EXP_CHROM_TO_XPOS_OFFSET.get(rows.locus.contig.replace('^chr', '')) + rows.locus.position,
    'cpx_intervals': lambda rows: hl.if_else(hl.is_defined(rows.info.CPX_INTERVALS),
                                             rows.info.CPX_INTERVALS.map(lambda x: get_cpx_interval(x)),
                                             hl.null(hl.dtype(INTERVAL_TYPE))),
}

COMPUTED_FIELDS = {
    'xstart': lambda rows: rows.xpos,
    'xstop': lambda rows: hl.if_else(hl.is_defined(rows.info.END2),
                                     EXP_CHROM_TO_XPOS_OFFSET.get(rows.info.CHR2.replace('^chr', '')) + rows.info.END2,
                                     EXP_CHROM_TO_XPOS_OFFSET.get(rows.locus.contig.replace('^chr', '')) + rows.info.END),
    'svType': lambda rows: rows.sv_type[0],
    'transcriptConsequenceTerms': lambda rows: [rows.sv_type[0]],
    'sv_type_detail': lambda rows: hl.if_else(rows.sv_type[0] == 'CPX', rows.info.CPX_TYPE,
                                              hl.if_else(rows.sv_type[0] == 'INS',
                                                         rows.sv_type[-1], hl.null('str'))),
    'sortedTranscriptConsequences': lambda rows: hl.flatmap(lambda x: x, rows.gene_affected),
    'geneIds': lambda rows: rows.gene_affected.filter(
        lambda x: x[0].predicted_consequence != 'NEAREST_TSS').map(lambda x: x[0].gene_id),
    'samples_no_call': lambda rows: rows.genotypes.filter(lambda x: ~hl.is_defined(x.num_alt)).map(lambda x: x.sample_id),
    'samples_num_alt_1': lambda rows: get_sample_num_alt_x(rows, 1),
    'samples_num_alt_2': lambda rows: get_sample_num_alt_x(rows, 2),
}

FIELDS = list(BASIC_FIELDS.keys()) + list(COMPUTED_FIELDS.keys()) + ['genotypes']


def get_sample_num_alt_x(rows, n):
    return rows.genotypes.filter(lambda x: x.num_alt == n).map(lambda x: x.sample_id)


def get_cpx_interval(x):
    # an example format of CPX_INTERVALS is "DUP_chr1:1499897-1499974"
    type_chr = x.split('_chr')
    chr_pos = type_chr[1].split(':')
    pos = chr_pos[1].split('-')
    return hl.struct(type=type_chr[0], chrom=chr_pos[0], start=hl.int32(pos[0]), end=hl.int32(pos[1]))


def load_mt(input_dataset, matrixtable_file, overwrite_matrixtable):
    if matrixtable_file == '':
        matrixtable_file = '{}.mt'.format(os.path.splitext(input_dataset)[0])

    # For the CMG dataset, we need to do hl.import_vcf() for once for all projects.
    if not overwrite_matrixtable and os.path.isdir(matrixtable_file):
        reminder = 'If the input VCF file has been changed, or you just want to re-import VCF,' \
                   ' please add "--overwrite-matrixtable" command line option.'
        logger.info('Use the existing MatrixTable file {}. {}'.format(matrixtable_file, reminder))
    else:
        hl.import_vcf(input_dataset, reference_genome='GRCh38').write(matrixtable_file)
        logger.info('The VCF file has been imported to the MatrixTable at {}.'.format(matrixtable_file))

    return hl.read_matrix_table(matrixtable_file)


def subset_mt(project_guid, mt, skip_sample_subset=False, ignore_missing_samples=False, sample_type=WGS_SAMPLE_TYPE):
    if not skip_sample_subset:
        sample_subset = get_sample_subset(project_guid, sample_type)
        found_samples = sample_subset.intersection({col.s for col in mt.cols().collect()})
        if len(found_samples) != len(sample_subset):
            missed_samples = sample_subset - found_samples
            missing_sample_message = 'Missing the following {} samples:\n{}'.format(
                len(missed_samples), ', '.join(sorted(missed_samples))
            )
            if ignore_missing_samples:
                logger.info(missing_sample_message)
            else:
                logger.error(missing_sample_message)
                raise Exception(missing_sample_message)

        sample_remap = get_sample_remap(project_guid, sample_type)
        message = 'Subsetting to {} samples'.format(len(sample_subset))
        if sample_remap:
            message += ' (remapping {} samples)'.format(len(sample_remap))
            sample_subset = sample_subset - set(sample_remap.keys())
            sample_subset.update(set(sample_remap.values()))
            mt = mt.key_cols_by()
            sample_remap = hl.literal(sample_remap)
            mt = mt.annotate_cols(s=hl.if_else(sample_remap.contains(mt.s), sample_remap[mt.s], mt.s))
        logger.info(message)

        mt = mt.filter_cols(hl.literal(sample_subset).contains(mt.s))

    genotypes = hl.agg.collect(hl.struct(sample_id=mt.s, gq=mt.GQ, num_alt=mt.GT.n_alt_alleles(), cn=mt.RD_CN))
    mt = mt.annotate_rows(genotypes=genotypes)
    return mt.filter_rows(mt.genotypes.any(lambda x: x.num_alt > 0)).rows()


def annotate_fields(rows, gene_id_mapping):
    rows = rows.annotate(**{k: v(rows) for k, v in BASIC_FIELDS.items()})

    rows = rows.annotate(
        gene_affected=hl.filter(
            lambda x: hl.is_defined(x),
            [rows.info[col].map(lambda gene: hl.struct(gene_symbol=gene, gene_id=gene_id_mapping[gene],
                                                       predicted_consequence=col.split('__')[-1]))
             for col in [gene_col for gene_col in rows.info if gene_col.startswith('PROTEIN_CODING__')
                         and rows.info[gene_col].dtype == hl.dtype('array<str>')]]),
        sv_type=rows.alleles[1].replace('[<>]', '').split(':', 2),
    )

    COMPUTED_FIELDS.update({'filters': lambda rows: hl.if_else(hl.len(rows.filters) > 0, rows.filters,
                                                               hl.null(hl.dtype('array<str>')))})
    rows = rows.annotate(**{k: v(rows) for k, v in COMPUTED_FIELDS.items()})

    rows = rows.rename({'rsid': 'variantId'})

    return rows.key_by('variantId').select(*FIELDS)


def export_to_es(rows, input_dataset, project_guid, es_host, es_port, block_size, num_shards):
    meta = {
      'genomeVersion': '38',
      'sampleType': WGS_SAMPLE_TYPE,
      'datasetType': 'SV',
      'sourceFilePath': input_dataset,
    }
    index_name = get_es_index_name(project_guid, meta)

    es_password = os.environ.get('PIPELINE_ES_PASSWORD', '')
    es_client = ElasticsearchClient(host=es_host, port=es_port, es_password=es_password)

    es_client.export_table_to_elasticsearch(
        rows,
        index_name=index_name,
        index_type_name='_doc',
        block_size=block_size,
        num_shards=num_shards,
        delete_index_before_exporting=True,
        export_globals_to_index_meta=True,
        verbose=True,
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument('input_dataset', help='input VCF file')
    p.add_argument('--matrixtable-file', default='', help='file name (includes path) of the MatrixTable for data imported from VCF input')
    p.add_argument('--overwrite-matrixtable', action='store_true', help='always import vcf data ignoring any existing matrixtable file')
    p.add_argument('--skip-sample-subset', action='store_true')
    p.add_argument('--ignore-missing-samples', action='store_true')
    p.add_argument('--project-guid', required=True, help='the guid of the target seqr project')
    p.add_argument('--gencode-release', type=int, default=29)
    p.add_argument('--gencode-path', default='', help='path for downloaded Gencode data')
    p.add_argument('--es-host', default='localhost')
    p.add_argument('--es-port', default='9200')
    p.add_argument('--num-shards', type=int, default=6)
    p.add_argument('--block-size', type=int, default=2000)

    args = p.parse_args()

    start_time = time.time()

    hl.init()

    mt = load_mt(args.input_dataset, args.matrixtable_file, args.overwrite_matrixtable)

    rows = subset_mt(args.project_guid, mt, sample_type=WGS_SAMPLE_TYPE, skip_sample_subset=args.skip_sample_subset,
                     ignore_missing_samples=args.ignore_missing_samples)

    gene_id_mapping = hl.literal(load_gencode(args.gencode_release, download_path=args.gencode_path))

    rows = annotate_fields(rows, gene_id_mapping)

    export_to_es(rows, args.input_dataset, args.project_guid, args.es_host, args.es_port, args.block_size, args.num_shards)
    logger.info('Total time for subsetting, annotating, and exporting: {}'.format(time.time() - start_time))

    hl.stop()


if __name__ == '__main__':
    main()
