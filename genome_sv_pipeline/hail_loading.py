import hail as hl
import logging
import time

from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient
from sv_pipeline.load_data import get_sample_subset, get_es_index_name, CHROM_TO_XPOS_OFFSET
from genome_sv_pipeline.mapping_gene_ids import load_gencode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

gene_id_mapping = {}


def sub_setting_mt(guid, mt):
    sample_subset = get_sample_subset(guid, 'WGS')
    logger.info('Total {} samples in project {}'.format(len(sample_subset), guid))
    mt1 = mt.filter_cols(hl.literal(sample_subset).contains(mt['s']))

    missing_samples = sample_subset - {col.s for col in mt1.key_cols_by().cols().collect()}
    logger.info('{} missing samples: {}'.format(len(missing_samples), missing_samples))

    genotypes = hl.agg.filter(mt1.GT.is_non_ref(), hl.agg.collect(hl.struct(sample_id=mt1.s, gq=mt1.GQ, num_alt=mt1.GT[0]+mt1.GT[1], cn=mt1.RD_CN)))

    mt2 = mt1.annotate_rows(genotypes=hl.if_else(hl.len(genotypes)>0, genotypes,
                                                 hl.null(hl.dtype('array<struct{sample_id: str, gq: int32, num_alt: int32, cn: int32}>'))))
    mt3 = mt2.filter_rows(hl.is_defined(mt2.genotypes))
    return mt3.rows()


def annotate_fields(rows):
    chrom_offset = hl.literal(CHROM_TO_XPOS_OFFSET)
    gene_cols = [gene_col for gene_col in rows.info if gene_col.startswith('PROTEIN_CODING__')]
    sv_type = rows.alleles[1].replace('[<>]', ' ').strip()
    contig = rows.locus.contig.split('chr')[1]
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
        'samples': hl.map(lambda x: x.sample_id, rows.genotypes),
    }

    rows = rows.annotate(**kwargs)

    sv_type = rows.alleles[1].replace('[<>]', ' ').strip()
    rows = rows.annotate(
        _svDetail_ins=hl.if_else(sv_type.startswith('INS:'), sv_type[4:], hl.null('str')),
        _sortedTranscriptConsequences=hl.filter(
            lambda x: hl.is_defined(x.genes),
            [hl.struct(genes=rows.info.get(col), predicted_consequence=col.split('__')[-1])
             for col in gene_cols if rows.info.get(col).dtype == hl.dtype('array<str>')]))

    temp_fields = {
        '_samples_cn_gte_4': hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.cn >= 4, rows.genotypes)),
        '_geneIds': hl.flatmap(lambda x: x.genes,
                               hl.filter(lambda x: x.predicted_consequence != 'NEAREST_TSS',
                                         rows._sortedTranscriptConsequences)),
                   }
    temp_fields.update({
        '_samples_cn_{}'.format(cnt): hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.cn == cnt, rows.genotypes))
        for cnt in range(4)})
    temp_fields.update({
        '_samples_num_alt_{}'.format(cnt): hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.num_alt == cnt, rows.genotypes))
        for cnt in range(3)})
    rows = rows.annotate(**temp_fields)

    interval_type = hl.dtype('array<struct{alt: str, chrom: str, start: int32, end: int32}>')
    other_fields = {
        'xstart': rows.xpos,
        'xstop': hl.if_else(hl.is_defined(rows.info.END2),
                            chrom_offset.get(rows.info.CHR2.split('chr')[1]) + rows.info.END2, rows.xpos),
        'sortedTranscriptConsequences':
            hl.flatmap(lambda x: hl.map(lambda y: hl.struct(gene_symbol=y, gene_id=gene_id_mapping[y],
                predicted_consequence=x.predicted_consequence), x.genes), rows._sortedTranscriptConsequences),
        'transcriptConsequenceTerms': [rows.svType],
        'svTypeDetail': hl.if_else(rows.svType=='CPX', rows.info.CPX_TYPE, rows._svDetail_ins),
        'cpxIntervals': hl.if_else(rows.svType=='CPX',
            hl.if_else(hl.is_defined(rows.info.CPX_INTERVALS),
                hl.map(lambda x: hl.struct(alt=x.split('_chr')[0], chrom=x.split('_chr')[1].split(':')[0],
                    start=hl.int32(x.split(':')[1].split('-')[0]),
                    end=hl.int32(x.split('-')[1])), rows.info.CPX_INTERVALS),
                hl.null(interval_type)),
            hl.null(interval_type)),
    }
    other_fields.update({field[1:]: hl.if_else(hl.len(rows[field])>0, rows[field],
                                           hl.null(hl.dtype('array<str>'))) for field in temp_fields.keys()})
    rows = rows.annotate(**other_fields)

    mapping = {
        'rsid': 'variantId',
    }
    rows = rows.rename(mapping)

    fields = list(kwargs.keys()) + ['filters'] + list(other_fields.keys()) + ['genotypes']
    return rows.key_by('variantId').select(*fields)


def measure_time(pre_time, message):
    current_time = time.time()
    print('Time for {}: {:.2f} seconds.'.format(message, current_time - pre_time))
    return current_time


def main():
    hl.init()

    start_time = time.time()
    pre_time = start_time

    global gene_id_mapping
    gene_id_mapping = hl.literal(load_gencode(29, genome_version='38'))

    pre_time = measure_time(pre_time, 'loading gene ID mapping table')

    input_dataset = 'vcf/sv.vcf.bgz'
    guid = 'R0332_cmg_estonia_wgs'
    # For the CMG dataset, we need to do hl.import_vcf() for once for all projects.
    # hl.import_vcf(input_dataset, reference_genome='GRCh38').write('vcf/svs.mt', overwrite=True)

    pre_time = measure_time(pre_time, 'converting VCF to MatrixTable')

    mt = hl.read_matrix_table('vcf/svs.mt')

    pre_time = measure_time(pre_time, 'loading MatrixTable')

    rows = sub_setting_mt(guid, mt)
    logger.info('Variant counts: {}'.format(rows.count()))

    pre_time = measure_time(pre_time, 'sub_setting sample data')

    rows = annotate_fields(rows)

    pre_time = measure_time(pre_time, 'annotating fields')

    sample_type = 'WGS'
    meta = {
      'genomeVersion': '38',
      'sampleType': sample_type,
      'datasetType': 'SV',
      'sourceFilePath': input_dataset,
    }
    index_name = get_es_index_name(guid, meta)

    es_host = '192.168.1.244'
    es_port = '9200'
    es_password = None
    index_type = '_doc'
    block_size = 2000
    num_shards = 1
    es_client = ElasticsearchClient(host=es_host, port=es_port, es_password=es_password)

    if es_client.es.indices.exists(index=index_name):
        logger.info('Deleting existing index')
        es_client.es.indices.delete(index=index_name)

    es_client.export_table_to_elasticsearch(
        rows,
        index_name=index_name,
        index_type_name=index_type,
        block_size=block_size,
        num_shards=num_shards,
        delete_index_before_exporting=True,
        export_globals_to_index_meta=True,
        verbose=True,
    )
    pre_time = measure_time(pre_time, 'exporting to Elasticsearch')
    print('Total time: {:.2f} minutes.'.format((pre_time - start_time)/60))

if __name__ == '__main__':
    main()

# Outputs:
# INFO:genome_sv_pipeline.mapping_gene_ids:Re-using /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29lift37.annotation.gtf.gz previously downloaded from http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_29/GRCh37_mapping/gencode.v29lift37.annotation.gtf.gz
# INFO:genome_sv_pipeline.mapping_gene_ids:Re-using /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29.annotation.gtf.gz previously downloaded from http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_29/gencode.v29.annotation.gtf.gz
# INFO:genome_sv_pipeline.mapping_gene_ids:Loading /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29lift37.annotation.gtf.gz (genome version: 37)
# 2753539 gencode records [00:11, 244226.96 gencode records/s]
# INFO:genome_sv_pipeline.mapping_gene_ids:Loading /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29.annotation.gtf.gz (genome version: 38)
# 2742022 gencode records [00:09, 283505.84 gencode records/s]
# INFO:genome_sv_pipeline.mapping_gene_ids:Get 59227 gene id mapping records
# Time for loading gene ID mapping table: 21.68 seconds.
# [Stage 0:=============================>                             (1 + 1) / 2]2021-04-05 11:42:09 Hail: INFO: Coerced almost-sorted dataset
# [Stage 1:=============================>                             (1 + 1) / 2]2021-04-05 11:43:29 Hail: INFO: wrote matrix table with 145568 rows and 714 columns in 2 partitions to vcf/svs.mt
#     Total size: 184.24 MiB
#     * Rows/entries: 184.24 MiB
#     * Columns: 4.09 KiB
#     * Globals: 11.00 B
#     * Smallest partition: 75119 rows (91.61 MiB)
#     * Largest partition:  70449 rows (92.63 MiB)
# Time for converting VCF to MatrixTable: 86.92 seconds.
# Time for loading MatrixTable: 0.13 seconds.
# INFO:__main__:Total 167 samples in project R0332_cmg_estonia_wgs
# INFO:__main__:61 missing samples: {'HK032_0081', 'HK112-001_1', 'HK015_0038_D2', 'HK117-002_1', 'HK115-002_1', 'HK085-006_D2', 'HK015_0036', 'HK100-002_D1', 'HK061-0157_D1', 'HK081-001_D2', 'HK108-003_1', 'HK079-001_D2', 'HK080-002_D2', 'HK119-001_1', 'OUN_HK131_001_D1', 'OUN_HK126_001_D1', 'HK035_0089', 'HK085-001_D2', 'HK080-001_D2', 'HK085-004_D2', 'OUN_HK132_001_D1', 'OUN_HK131_003_D1', 'HK081-002_D2', 'HK117-001_1', 'HK081-003_D2', 'HK100-003_D1', 'HK117-003_1', 'HK100-004_D1', 'E00859946', 'OUN_HK124_001_D1', 'OUN_HK132_003_D1', 'HK080-003_D2', 'HK115-003_1', 'HK017-0044', 'HK032_0081_2_D2', 'HK108-002_1', 'HK060-0156_1', 'HK079-003_D2', 'HK017-0045', 'OUN_HK124_003_D1', 'HK108-001_1', 'OUN_HK131_002_D1', 'OUN_HK126_002_D1', 'HK104-001_D2', 'HK060-0154_1', 'HK119-002_1', 'HK112-002_1', 'HK079-002_D2', 'HK115-001_1', 'HK061-0159_D1', 'HK104-002_D2', 'HK017-0046', 'HK112-003_1', 'HK119-003_1', 'HK060-0155_1', 'HK100-001_D1', 'HK085-002_D2', 'OUN_HK124_002_D1', 'OUN_HK132_002_D1', 'OUN_HK126_003_D1', 'HK061-0158_D1'}
# [Stage 2:=============================>                             (1 + 1) / 2]INFO:__main__:Variant counts: 67275
# Time for sub_setting sample data: 7.54 seconds.
# Time for annotating fields: 0.11 seconds.
# INFO:elasticsearch:GET http://192.168.1.244:9200/ [status:200 request:0.005s]
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
# INFO:elasticsearch:HEAD http://192.168.1.244:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210405 [status:200 request:0.003s]
# INFO:__main__:Deleting existing index
# INFO:elasticsearch:DELETE http://192.168.1.244:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210405 [status:200 request:0.184s]
# INFO:root: struct {
#      variantId: str,
#      filters: set<str>,
#      contig: str,
#      sc: int32,
#      sf: float64,
#      sn: int32,
#      svType: str,
#      start: int32,
#      end: int32,
#      sv_callset_Hemi: int32,
#      sv_callset_Hom: int32,
#      gnomad_svs_ID: str,
#      gnomad_svs_AF: float64,
#      pos: int32,
#      xpos: int64,
#      xstart: int64,
#      xstop: int64,
#      samples: array<str>,
#      samples_num_alt_0: array<str>,
#      samples_num_alt_1: array<str>,
#      samples_num_alt_2: array<str>,
#      samples_cn_0: array<str>,
#      samples_cn_1: array<str>,
#      samples_cn_2: array<str>,
#      samples_cn_3: array<str>,
#      samples_cn_gte_4: array<str>,
#      geneIds: array<str>,
#      sortedTranscriptConsequences: array<struct {
#          gene_symbol: str,
#          gene_id: str,
#          predicted_consequence: str
#      }>,
#      transcriptConsequenceTerms: array<str>,
#      svTypeDetail: str,
#      cpxIntervals: array<struct {
#          alt: str,
#          chrom: str,
#          start: int32,
#          end: int32
#      }>,
#      genotypes: array<struct {
#          sample_id: str,
#          gq: int32,
#          num_alt: int32,
#          cn: int32
#      }>
#  }
# INFO:root:create_mapping - elasticsearch schema:
# {'contig': {'type': 'keyword'},
#  'cpxIntervals': {'properties': {'alt': {'type': 'keyword'},
#                                  'chrom': {'type': 'keyword'},
#                                  'end': {'type': 'integer'},
#                                  'start': {'type': 'integer'}},
#                   'type': 'nested'},
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
#  'locus': {'properties': {'contig': {'type': 'keyword'},
#                           'position': {'type': 'integer'}},
#            'type': 'object'},
#  'pos': {'type': 'integer'},
#  'samples': {'type': 'keyword'},
#  'samples_cn_0': {'type': 'keyword'},
#  'samples_cn_1': {'type': 'keyword'},
#  'samples_cn_2': {'type': 'keyword'},
#  'samples_cn_3': {'type': 'keyword'},
#  'samples_cn_gte_4': {'type': 'keyword'},
#  'samples_num_alt_0': {'type': 'keyword'},
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
#  'svTypeDetail': {'type': 'keyword'},
#  'sv_callset_Hemi': {'type': 'integer'},
#  'sv_callset_Hom': {'type': 'integer'},
#  'transcriptConsequenceTerms': {'type': 'keyword'},
#  'variantId': {'type': 'keyword'},
#  'xpos': {'type': 'long'},
#  'xstart': {'type': 'long'},
#  'xstop': {'type': 'long'}}
# INFO:root:==> creating elasticsearch index r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210405
# INFO:elasticsearch:PUT http://192.168.1.244:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210405 [status:200 request:0.670s]
# INFO:root:==> exporting data to elasticsearch. Write mode: index, blocksize: 2000
# Config Map(es.batch.size.entries -> 2000, es.index.auto.create -> true, es.write.operation -> index, es.port -> 9200, es.nodes -> 192.168.1.244)
# [Stage 3:=============================>                             (1 + 1) / 2]INFO:elasticsearch:POST http://192.168.1.244:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210405/_forcemerge [status:200 request:0.111s]
# Time for exporting to Elasticsearch: 61.34 seconds.
# Total time: 2.96 minutes.
