import argparse
import os
import hail as hl
import logging

from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient

from sv_pipeline.utils.common import get_sample_subset, get_es_index_name, CHROM_TO_XPOS_OFFSET
from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

gene_id_mapping = {}


def sub_setting_mt(guid, mt):
    sample_subset = get_sample_subset(guid, 'WGS')
    logger.info('Total {} samples in project {}'.format(len(sample_subset), guid))
    mt = mt.filter_cols(hl.literal(sample_subset).contains(mt['s']))

    missing_samples = sample_subset - {col.s for col in mt.key_cols_by().cols().collect()}
    message = '{} missing samples: {}'.format(len(missing_samples), missing_samples) if len(missing_samples)\
        else 'No missing samples.'
    logger.info(message)

    genotypes = hl.agg.filter(mt.GT.is_non_ref(), hl.agg.collect(hl.struct(sample_id=mt.s, gq=mt.GQ, num_alt=mt.GT.n_alt_alleles(), cn=mt.RD_CN)))

    mt = mt.annotate_rows(genotypes=hl.if_else(hl.len(genotypes) > 0, genotypes,
                                                 hl.null(hl.dtype('array<struct{sample_id: str, gq: int32, num_alt: int32, cn: int32}>'))))
    return mt.filter_rows(hl.is_defined(mt.genotypes)).rows()


def _annotate_basic_fields(rows):
    chrom_offset = hl.literal(CHROM_TO_XPOS_OFFSET)
    sv_type = rows.alleles[1].replace('[<>]', '')
    contig = rows.locus.contig.replace('^chr', '')
    kwargs = {
        'contig': contig,
        'sc': rows.info.AC[0],
        'sf': rows.info.AF[0],
        'sn': rows.info.AN,
        'svType': hl.if_else(sv_type.startswith('INS:'), 'INS', sv_type),
        'start': rows.locus.position,
        'end': rows.info.END,
        'sv_callset_Hemi': rows.info.N_HET,
        'sv_callset_Hom': rows.info.N_HOMALT,
        'gnomad_svs_ID': rows.info.gnomAD_V2_SVID,
        'gnomad_svs_AF': rows.info.gnomAD_V2_AF,
        'pos': rows.locus.position,
        'xpos': chrom_offset.get(contig) + rows.locus.position,
        'samples': rows.genotypes.map(lambda x: x.sample_id),
    }
    return list(kwargs.keys()), rows.annotate(**kwargs)


def _annotate_temp_fields(rows):
    sv_type = rows.alleles[1].replace('[<>]', ' ').strip()
    gene_cols = [gene_col for gene_col in rows.info if gene_col.startswith('PROTEIN_CODING__')]
    rows = rows.annotate(
        _svDetail_ins=hl.if_else(sv_type.startswith('INS:'), sv_type[4:], hl.null('str')),
        _sortedTranscriptConsequences=hl.filter(
            lambda x: hl.is_defined(x.genes),
            [hl.struct(genes=rows.info.get(col), predicted_consequence=col.split('__')[-1])
             for col in gene_cols if rows.info.get(col).dtype == hl.dtype('array<str>')]))

    temp_fields = {
        '_samples_cn_gte_4': rows.genotypes.filter(lambda x: x.cn >= 4).map(lambda x: x.sample_id),
        '_geneIds': rows._sortedTranscriptConsequences.filter(lambda x: x.predicted_consequence != 'NEAREST_TSS').flatmap(lambda x: x.genes),
        '_filters': hl.array(rows.filters.filter(lambda x: x != 'PASS')),
    }
    field_counts = [('cn', 4), ('num_alt', 3)]
    for field, count in field_counts:
        temp_fields.update({
            '_samples_{}_{}'.format(field, cnt): rows.genotypes.filter(lambda x: x[field] == cnt).map(lambda x: x.sample_id)
            for cnt in range(count)})
    return temp_fields.keys(), rows.annotate(**temp_fields)


def annotate_fields(rows):
    basic_fields, rows = _annotate_basic_fields(rows)

    temp_fields, rows = _annotate_temp_fields(rows)

    chrom_offset = hl.literal(CHROM_TO_XPOS_OFFSET)
    interval_type = hl.dtype('array<struct{type: str, chrom: str, start: int32, end: int32}>')
    other_fields = {
        'xstart': rows.xpos,
        'xstop': hl.if_else(hl.is_defined(rows.info.END2),
                            chrom_offset.get(rows.info.CHR2.replace('^chr', '')) + rows.info.END2, rows.xpos),
        'sortedTranscriptConsequences':
            rows._sortedTranscriptConsequences.flatmap(
                lambda x: x.genes.map(
                    lambda y: hl.struct(
                        gene_symbol=y,
                        gene_id=gene_id_mapping[y],
                        predicted_consequence=x.predicted_consequence))),
        'transcriptConsequenceTerms': [rows.svType],
        'svTypeDetail': hl.if_else(rows.svType == 'CPX', rows.info.CPX_TYPE, rows._svDetail_ins),
        'cpxIntervals': hl.if_else(hl.is_defined(rows.info.CPX_INTERVALS),
                                   rows.info.CPX_INTERVALS.map(lambda x: hl.struct(type=x.split('_chr')[0],
                                                              chrom=x.split('_chr')[1].split(':')[0],
                                                              start=hl.int32(x.split(':')[1].split('-')[0]),
                                                              end=hl.int32(x.split('-')[1]))),
                                   hl.null(interval_type)),
    }

    # remove empty temp fields
    other_fields.update({field[1:]: hl.if_else(hl.len(rows[field]) > 0, rows[field],
                                               hl.null(hl.dtype('array<str>'))) for field in temp_fields})
    rows = rows.annotate(**other_fields)

    rows = rows.rename({'rsid': 'variantId'})

    fields = basic_fields + list(other_fields.keys()) + ['genotypes']
    return rows.key_by('variantId').select(*fields)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('input_dataset', help='input VCF file')
    p.add_argument('--matrixtable-path', default='', help='path for Hail MatrixTable data')
    p.add_argument('--skip-sample-subset', action='store_true')
    p.add_argument('--ignore-missing-samples', action='store_true')
    p.add_argument('--project-guid')
    p.add_argument('--gencode-release', default=29)
    p.add_argument('--gencode-path', default='', help='path for downloaded Gencode data')
    p.add_argument('--sample-type', default='WGS')
    p.add_argument('--es-host', default='localhost')
    p.add_argument('--es-port', default='9200')
    p.add_argument('--num-shards', default=6)
    p.add_argument('--block-size', default=2000)

    args = p.parse_args()

    if args.matrixtable_path == '':
        args.matrixtable_path = '{}.mt'.format(args.input_dataset.split('.vcf')[0])

    hl.init()

    global gene_id_mapping
    gene_id_mapping = hl.literal(load_gencode(args.gencode_release, genome_version=args.sample_type, download_path=args.gencode_path))

    # For the CMG dataset, we need to do hl.import_vcf() for once for all projects.
    if os.path.isdir(args.matrixtable_path):
        logger.info('Use the existing MatrixTable {}.'.format(args.matrixtable_path))
    else:
        hl.import_vcf(args.input_dataset, reference_genome='GRCh38').write(args.matrixtable_path)

    mt = hl.read_matrix_table(args.matrixtable_path)

    rows = sub_setting_mt(args.project_guid, mt)
    logger.info('Variant counts: {}'.format(rows.count()))

    rows = annotate_fields(rows)

    meta = {
      'genomeVersion': '38',
      'sampleType': args.sample_type,
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


if __name__ == '__main__':
    main()
