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

    mt2 = mt1.annotate_rows(genotypes=genotypes)
    mt3 = mt2.filter_rows(mt2.genotypes != hl.empty_array(hl.dtype('struct{sample_id: str, gq: int32, num_alt: int32, cn: int32}')))
    return mt3.rows()


def annotate_fields(rows):
    chrom_offset = hl.literal(CHROM_TO_XPOS_OFFSET)
    gene_cols = [gene_col for gene_col in rows.info if gene_col.startswith('PROTEIN_CODING__')]
    kwargs = {
        'contig': rows.locus.contig.split('chr')[1],
        'sc': rows.info.AC[0],
        'sf': rows.info.AF[0],
        'sn': rows.info.AN,
        'svType': hl.if_else(rows.alleles[1].replace('[<>]', ' ').strip().startswith('INS:'), 'INS', rows.alleles[1].replace('[<>]', ' ').strip()),
        'svDetail_ins': hl.if_else(rows.alleles[1].replace('[<>]', ' ').strip().startswith('INS:'), rows.alleles[1].replace('[<>]', ' ').strip()[4:], hl.null('str')),
        'start': rows.locus.position,
        'end': rows.info.END,
        'sv_callset_Hemi': rows.info.N_HET,
        'sv_callset_Hom': rows.info.N_HOMALT,
        'gnomad_svs_ID': rows.info.gnomAD_V2_SVID,
        'gnomad_svs_AF': rows.info.gnomAD_V2_AF,
        'pos': rows.locus.position,
        'xpos': chrom_offset.get(rows.locus.contig.split('chr')[1]) + rows.locus.position,
        'xstart': chrom_offset.get(rows.locus.contig.split('chr')[1]) + rows.locus.position,
        'xstop': hl.if_else(hl.is_defined(rows.info.END2), chrom_offset.get(rows.info.CHR2.split('chr')[1]) + rows.info.END2,
                            chrom_offset.get(rows.locus.contig.split('chr')[1]) + rows.locus.position),
        'sortedTranscriptConsequences':
            hl.filter(lambda x: hl.is_defined(x.genes),
                      [hl.struct(genes=rows.info.get(col), predicted_consequence=col.split('__')[-1])
                       for col in gene_cols if rows.info.get(col).dtype == hl.dtype('array<str>')]),
        'samples': hl.map(lambda x: x.sample_id, rows.genotypes),
        'samples_num_alt_0': hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.num_alt == 0, rows.genotypes)),
        'samples_num_alt_1': hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.num_alt == 1, rows.genotypes)),
        'samples_num_alt_2': hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.num_alt == 2, rows.genotypes)),
        'samples_cn_0': hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.cn == 0, rows.genotypes)),
        'samples_cn_1': hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.cn == 1, rows.genotypes)),
        'samples_cn_2': hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.cn == 2, rows.genotypes)),
        'samples_cn_3': hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.cn == 3, rows.genotypes)),
        'samples_cn_gte_4': hl.map(lambda x: x.sample_id, hl.filter(lambda x: x.cn >= 4, rows.genotypes)),
    }

    rows = rows.annotate(**kwargs)

    interval_type = hl.dtype('array<struct{alt: str, chrom: str, start: int32, end: int32}>')
    other_fields = {
        'geneIds': hl.flatmap(lambda x: x.genes, hl.filter(lambda x: x.predicted_consequence != 'NEAREST_TSS',
                                                      rows.sortedTranscriptConsequences)),
        'transcriptConsequenceTerms': [rows.svType],
        'detailType': hl.if_else(rows.svType=='CPX', rows.info.CPX_TYPE,
                                 hl.if_else(hl.is_defined(rows.svDetail_ins), rows.svDetail_ins, hl.null('str'))),
        'cpxIntervals': hl.if_else(rows.svType=='CPX',
                hl.if_else(hl.is_defined(rows.info.CPX_INTERVALS), hl.map(lambda x: hl.struct(
                    alt=x.split('_chr')[0], chrom=x.split('_chr')[1].split(':')[0],
                    start=hl.int32(x.split('_chr')[1].split(':')[1].split('-')[0]),
                    end=hl.int32(x.split('_chr')[1].split(':')[1].split('-')[1])),
                    rows.info.CPX_INTERVALS), hl.null(interval_type)),
                hl.null(interval_type)),
    }
    rows = rows.annotate(**other_fields)

    mapping = {
        'rsid': 'variantId',
    }
    rows = rows.rename(mapping)

    fields = list(mapping.values()) + ['filters'] + list(kwargs.keys()) + list(other_fields.keys()) + ['genotypes']
    rows = rows.key_by('locus').select(*fields)
    return rows.drop('svDetail_ins')


def main():
    start_time = time.time()

    global gene_id_mapping
    gene_id_mapping = load_gencode(29, genome_version='38')

    mapping_time = time.time()
    print('Time for loading gene ID mapping table: {:.2f} seconds.'.format(mapping_time - start_time))

    hl.init()
    input_dataset = 'vcf/sv.vcf.gz'
    guid = 'R0332_cmg_estonia_wgs'
    # hl.import_vcf(input_dataset, force=True, reference_genome='GRCh38').write('vcf/svs.mt', overwrite=True)
    mt = hl.read_matrix_table('vcf/svs.mt')
    rows = sub_setting_mt(guid, mt)
    logger.info('Variant counts: {}'.format(rows.count()))

    rows = annotate_fields(rows)

    annotation_time = time.time()
    print('Time for annotating fields: {:.2f} seconds.'.format(annotation_time - mapping_time))

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
    export_es_time = time.time()
    print('Time for exporting to Elasticsearch: {:.2f} seconds.'.format(export_es_time - annotation_time))

if __name__ == '__main__':
    main()

# Outputs:
# INFO:genome_sv_pipeline.mapping_gene_ids:Re-using /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29lift37.annotation.gtf.gz previously downloaded from http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_29/GRCh37_mapping/gencode.v29lift37.annotation.gtf.gz
# INFO:genome_sv_pipeline.mapping_gene_ids:Re-using /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29.annotation.gtf.gz previously downloaded from http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_29/gencode.v29.annotation.gtf.gz
# INFO:genome_sv_pipeline.mapping_gene_ids:Loading /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29lift37.annotation.gtf.gz (genome version: 37)
# 2753539 gencode records [00:11, 236703.74 gencode records/s]
# INFO:genome_sv_pipeline.mapping_gene_ids:Loading /var/folders/p8/c2yjwplx5n5c8z8s5c91ddqc0000gq/T/gencode.v29.annotation.gtf.gz (genome version: 38)
# 2742022 gencode records [00:11, 234215.04 gencode records/s]
# INFO:genome_sv_pipeline.mapping_gene_ids:Get 59227 gene id mapping records
# Time for loading gene ID mapping table: 23.75 seconds.
# 2021-04-02 15:53:38 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# Setting default log level to "WARN".
# To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
# 2021-04-02 15:53:39 WARN  Hail:37 - This Hail JAR was compiled for Spark 2.4.5, running with Spark 2.4.1.
#   Compatibility is not guaranteed.
# Running on Apache Spark version 2.4.1
# SparkUI available at http://wm598-921.fios-router.home:4040
# Welcome to
#      __  __     <>__
#     / /_/ /__  __/ /
#    / __  / _ `/ / /
#   /_/ /_/\_,_/_/_/   version 0.2.61-3c86d3ba497a
# LOGGING: writing to /Users/shifa/dev/hail_elasticsearch_pipelines/genome_sv_pipeline/hail-20210402-1553-0.2.61-3c86d3ba497a.log
# INFO:__main__:Total 167 samples in project R0332_cmg_estonia_wgs
# INFO:__main__:61 missing samples: {'HK079-002_D2', 'HK104-002_D2', 'HK032_0081', 'HK085-004_D2', 'HK085-006_D2', 'OUN_HK132_003_D1', 'OUN_HK126_003_D1', 'HK085-001_D2', 'HK017-0044', 'HK060-0154_1', 'HK108-002_1', 'HK115-001_1', 'HK115-003_1', 'HK100-004_D1', 'HK080-002_D2', 'OUN_HK132_002_D1', 'HK108-003_1', 'HK085-002_D2', 'OUN_HK132_001_D1', 'HK119-001_1', 'OUN_HK126_002_D1', 'HK080-003_D2', 'HK100-002_D1', 'HK100-001_D1', 'HK108-001_1', 'HK017-0045', 'OUN_HK124_001_D1', 'HK015_0036', 'HK061-0159_D1', 'OUN_HK124_003_D1', 'HK060-0156_1', 'HK060-0155_1', 'HK117-003_1', 'HK080-001_D2', 'OUN_HK126_001_D1', 'HK035_0089', 'HK032_0081_2_D2', 'HK017-0046', 'HK079-003_D2', 'HK061-0157_D1', 'HK015_0038_D2', 'HK112-002_1', 'HK081-001_D2', 'HK115-002_1', 'HK079-001_D2', 'OUN_HK131_003_D1', 'HK061-0158_D1', 'HK119-003_1', 'OUN_HK124_002_D1', 'HK112-003_1', 'E00859946', 'HK081-002_D2', 'HK100-003_D1', 'HK119-002_1', 'HK104-001_D2', 'OUN_HK131_001_D1', 'HK081-003_D2', 'HK112-001_1', 'HK117-001_1', 'HK117-002_1', 'OUN_HK131_002_D1'}
# [Stage 0:>                                                          (0 + 1) / 1]INFO:__main__:Variant counts: 67275
# Time for annotating fields: 17.93 seconds.
# INFO:elasticsearch:GET http://192.168.1.244:9200/ [status:200 request:0.021s]
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
# INFO:elasticsearch:HEAD http://192.168.1.244:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402 [status:200 request:0.010s]
# INFO:__main__:Deleting existing index
# INFO:elasticsearch:DELETE http://192.168.1.244:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402 [status:200 request:0.850s]
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
#      sortedTranscriptConsequences: array<struct {
#          genes: array<str>,
#          predicted_consequence: str
#      }>,
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
#      transcriptConsequenceTerms: array<str>,
#      detailType: str,
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
#  'sortedTranscriptConsequences': {'properties': {'genes': {'type': 'keyword'},
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
# INFO:elasticsearch:PUT http://192.168.1.244:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402 [status:200 request:0.973s]
# INFO:root:==> exporting data to elasticsearch. Write mode: index, blocksize: 2000
# Config Map(es.batch.size.entries -> 2000, es.index.auto.create -> true, es.write.operation -> index, es.port -> 9200, es.nodes -> 192.168.1.244)
# [Stage 1:>                                                          (0 + 1) / 1]INFO:elasticsearch:POST http://192.168.1.244:9200/r0332_cmg_estonia_wgs__structural_variants__wgs__grch38__20210402/_forcemerge [status:200 request:0.086s]
# Time for exporting to Elasticsearch: 81.46 seconds.