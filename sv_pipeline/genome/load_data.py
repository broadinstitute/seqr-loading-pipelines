import argparse
import hail as hl
import logging
import os
import time

from hail_scripts.v02.utils.elasticsearch_client import HailElasticsearchClient

from sv_pipeline.utils.common import get_sample_subset, get_sample_remap, get_es_index_name, CHROM_TO_XPOS_OFFSET
from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EXP_CHROM_TO_XPOS_OFFSET = hl.literal(CHROM_TO_XPOS_OFFSET)

TRANS_CONSEQ_TERMS = 'transcriptConsequenceTerms'
SORTED_TRANS_CONSEQ = 'sortedTranscriptConsequences'
SV_TYPE = 'sv_type'
MAJOR_CONSEQ = 'major_consequence'
GQ_BIN_SIZE = 10
WGS_SAMPLE_TYPE = 'WGS'

INTERVAL_TYPE = 'array<struct{type: str, chrom: str, start: int32, end: int32}>'

CORE_FIELDS = {
    'contig': lambda rows: rows.locus.contig.replace('^chr', ''),
    'sc': lambda rows: rows.info.AC[0],
    'sf': lambda rows: rows.info.AF[0],
    'sn': lambda rows: rows.info.AN,
    'start': lambda rows: rows.locus.position,
    'end': lambda rows: hl.if_else(hl.is_defined(rows.info.END2), rows.info.END2, rows.info.END),
    'sv_callset_Het': lambda rows: rows.info.N_HET,
    'sv_callset_Hom': lambda rows: rows.info.N_HOMALT,
    'gnomad_svs_ID': lambda rows: rows.info['gnomad_v2.1_sv_SVID'],
    'gnomad_svs_AF': lambda rows: rows.info['gnomad_v2.1_sv_AF'],
    'pos': lambda rows: rows.locus.position,
    'filters': lambda rows: hl.array(rows.filters.filter(lambda x: x != 'PASS')),
    'algorithms': lambda rows: rows.info.ALGORITHMS,
    'StrVCTVRE_score': lambda rows: rows.info.StrVCTVRE,
    'xpos': lambda rows: get_xpos(rows.locus.contig, rows.locus.position),
    'cpx_intervals': lambda rows: hl.if_else(hl.is_defined(rows.info.CPX_INTERVALS),
                                             rows.info.CPX_INTERVALS.map(lambda x: get_cpx_interval(x)),
                                             hl.missing(hl.dtype(INTERVAL_TYPE))),
}

DERIVED_FIELDS = {
    'xstart': lambda rows: rows.xpos,
    'xstop': lambda rows: hl.if_else(hl.is_defined(rows.info.END2),
                                     get_xpos(rows.info.CHR2, rows.info.END2),
                                     get_xpos(rows.locus.contig, rows.info.END)),
    'svType': lambda rows: rows[SV_TYPE][0],
    TRANS_CONSEQ_TERMS: lambda rows: rows[SORTED_TRANS_CONSEQ].map(lambda conseq: conseq[MAJOR_CONSEQ]).extend([rows[SV_TYPE][0]]),
    'sv_type_detail': lambda rows: hl.if_else(rows[SV_TYPE][0] == 'CPX', rows.info.CPX_TYPE,
                                              hl.if_else((rows[SV_TYPE][0] == 'INS') & (hl.len(rows[SV_TYPE]) > 1),
                                                         rows[SV_TYPE][1], hl.missing('str'))),
    'geneIds': lambda rows: hl.set(hl.map(lambda x: x.gene_id, rows.sortedTranscriptConsequences.filter(
        lambda x: x[MAJOR_CONSEQ] != 'NEAREST_TSS'))),
    'samples_no_call': lambda rows: get_sample_num_alt_x(rows, -1),
    'samples_num_alt_1': lambda rows: get_sample_num_alt_x(rows, 1),
    'samples_num_alt_2': lambda rows: get_sample_num_alt_x(rows, 2),
}

SAMPLES_GQ_FIELDS = {'samples_gq_sv_{}_to_{}'.format(i, i+GQ_BIN_SIZE): i for i in range(0, 1000, GQ_BIN_SIZE)}

FIELDS = list(CORE_FIELDS.keys()) + list(DERIVED_FIELDS.keys()) + ['variantId', SORTED_TRANS_CONSEQ, 'genotypes'] +\
    list(SAMPLES_GQ_FIELDS.keys())


def get_xpos(contig, pos):
    return EXP_CHROM_TO_XPOS_OFFSET.get(contig.replace('^chr', '')) + pos


def get_sample_num_alt_x(rows, n):
    return rows.genotypes.filter(lambda x: x.num_alt == n).map(lambda x: x.sample_id)


def get_sample_in_gq_range(rows, start, end):
    samples = rows.genotypes.filter(lambda x: (x.gq >= start) & (x.gq < end)).map(lambda x: x.sample_id)
    return hl.if_else(hl.len(samples) > 0, samples, hl.missing(hl.dtype('array<str>')))


def get_cpx_interval(x):
    # an example format of CPX_INTERVALS is "DUP_chr1:1499897-1499974"
    type_chr = x.split('_chr')
    chr_pos = type_chr[1].split(':')
    pos = chr_pos[1].split('-')
    return hl.struct(type=type_chr[0], chrom=chr_pos[0], start=hl.int32(pos[0]), end=hl.int32(pos[1]))


def load_mt(input_dataset, matrixtable_file, overwrite_matrixtable):
    if not matrixtable_file:
        matrixtable_file = '{}.mt'.format(os.path.splitext(input_dataset)[0])

    # For the CMG dataset, we need to do hl.import_vcf() for once for all projects.
    if not overwrite_matrixtable and os.path.isdir(matrixtable_file):
        reminder = 'If the input VCF file has been changed, or you just want to re-import VCF,' \
                   ' please add "--overwrite-matrixtable" command line option.'
        logger.info('Use the existing MatrixTable file {}. {}'.format(matrixtable_file, reminder))
    else:
        hl.import_vcf(input_dataset, reference_genome='GRCh38').write(matrixtable_file, overwrite=True)
        logger.info('The VCF file has been imported to the MatrixTable at {}.'.format(matrixtable_file))

    return hl.read_matrix_table(matrixtable_file)


def subset_mt(project_guid, mt, skip_sample_subset=False, ignore_missing_samples=False, id_file=None):
    if not skip_sample_subset:
        sample_subset = get_sample_subset(project_guid, WGS_SAMPLE_TYPE, filename=id_file)
        found_samples = sample_subset.intersection(mt.aggregate_cols(hl.agg.collect_as_set(mt.s)))
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

        sample_remap = get_sample_remap(project_guid, WGS_SAMPLE_TYPE)
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

    return mt.filter_rows(hl.agg.any(mt.GT.is_non_ref()))


def annotate_fields(mt, gencode_release, gencode_path):
    genotypes = hl.agg.collect(hl.struct(sample_id=mt.s, gq=mt.GQ, cn=mt.RD_CN,
                                         num_alt=hl.if_else(hl.is_defined(mt.GT), mt.GT.n_alt_alleles(), -1)))
    rows = mt.annotate_rows(genotypes=genotypes).rows()

    rows = rows.annotate(**{k: v(rows) for k, v in CORE_FIELDS.items()})

    gene_id_mapping = hl.literal(load_gencode(gencode_release, download_path=gencode_path))

    rows = rows.annotate(**{
        SORTED_TRANS_CONSEQ: hl.flatmap(lambda x: x, hl.filter(
            lambda x: hl.is_defined(x),
            [rows.info[col].map(lambda gene: hl.struct(**{'gene_symbol': gene, 'gene_id': gene_id_mapping[gene],
                                                       MAJOR_CONSEQ: col.split('__')[-1]}))
             for col in [gene_col for gene_col in rows.info if gene_col.startswith('PROTEIN_CODING__')
                         and rows.info[gene_col].dtype == hl.dtype('array<str>')]])),
        SV_TYPE: rows.alleles[1].replace('[<>]', '').split(':', 2)}
    )

    DERIVED_FIELDS.update({'filters': lambda rows: hl.if_else(hl.len(rows.filters) > 0, rows.filters,
                                                                 hl.missing(hl.dtype('array<str>')))})
    rows = rows.annotate(**{k: v(rows) for k, v in DERIVED_FIELDS.items()})

    rows = rows.annotate(**{k: get_sample_in_gq_range(rows, i, i+GQ_BIN_SIZE) for k, i in SAMPLES_GQ_FIELDS.items()})

    rows = rows.rename({'rsid': 'variantId'})

    return rows.key_by().select(*FIELDS)


def export_to_es(rows, input_dataset, project_guid, es_host, es_port, block_size, num_shards, es_nodes_wan_only):
    meta = {
      'genomeVersion': '38',
      'sampleType': WGS_SAMPLE_TYPE,
      'datasetType': 'SV',
      'sourceFilePath': input_dataset,
    }

    index_name = get_es_index_name(project_guid, meta)

    rows = rows.annotate_globals(**meta)

    es_password = os.environ.get('PIPELINE_ES_PASSWORD', '')
    es_client = HailElasticsearchClient(host=es_host, port=es_port, es_password=es_password)

    es_client.export_table_to_elasticsearch(
        rows,
        index_name=index_name,
        block_size=block_size,
        num_shards=num_shards,
        delete_index_before_exporting=True,
        export_globals_to_index_meta=True,
        verbose=True,
        elasticsearch_config={'es.nodes.wan.only': es_nodes_wan_only}
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument('input_dataset', help='input VCF file')
    p.add_argument('--matrixtable-file', help='file name (includes path) of the MatrixTable for data imported from VCF input')
    p.add_argument('--overwrite-matrixtable', action='store_true', help='always import vcf data ignoring any existing matrixtable file')
    p.add_argument('--skip-sample-subset', action='store_true')
    p.add_argument('--ignore-missing-samples', action='store_true')
    p.add_argument('--project-guid', required=True, help='the guid of the target seqr project')
    p.add_argument('--gencode-release', type=int, default=29)
    p.add_argument('--gencode-path', help='path for downloaded Gencode data')
    p.add_argument('--es-host', default='localhost')
    p.add_argument('--es-port', default='9200')
    p.add_argument('--num-shards', type=int, default=1)
    p.add_argument('--block-size', type=int, default=2000)
    p.add_argument('--es-nodes-wan-only', action='store_true')
    p.add_argument('--id-file', help='The full path (can start with gs://) of the id file. Should only be used for testing purposes, not intended for use in production')

    args = p.parse_args()

    start_time = time.time()

    hl.init()

    mt = load_mt(args.input_dataset, args.matrixtable_file, args.overwrite_matrixtable)

    mt = subset_mt(args.project_guid, mt, skip_sample_subset=args.skip_sample_subset,
                   ignore_missing_samples=args.ignore_missing_samples,
                   id_file=args.id_file)

    rows = annotate_fields(mt, args.gencode_release, args.gencode_path)

    export_to_es(rows, args.input_dataset, args.project_guid, args.es_host, args.es_port, args.block_size,
                 args.num_shards, 'true' if args.es_nodes_wan_only else 'false')
    logger.info('Total time for subsetting, annotating, and exporting: {}'.format(time.time() - start_time))

    hl.stop()


if __name__ == '__main__':
    main()
