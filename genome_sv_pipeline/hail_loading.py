import hail as hl
import logging
from tqdm import tqdm

from sv_pipeline.load_data import get_sample_subset
from genome_sv_pipeline.load_data import GENES_COLUMNS, GENES_FIELD

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sub_setting_mt(guid, mt):
    sample_subset = get_sample_subset(guid, 'WGS')
    logger.info('Total {} samples in project {}'.format(len(sample_subset), guid))
    mt1 = mt.filter_cols(hl.literal(sample_subset).contains(mt['s']))

    missing_samples = sample_subset - {col.s for col in mt1.key_cols_by().cols().collect()}
    logger.info('{} missing samples: {}'.format(len(missing_samples), missing_samples))

    samples = hl.agg.filter(mt1.GT.is_non_ref(), hl.agg.collect(hl.struct(id=mt1.s, gq=mt1.GQ, cn=mt1.RD_CN)))
    mt2 = mt1.annotate_rows(samples=samples)
    mt3 = mt2.filter_rows(mt2.samples != hl.empty_array(hl.dtype('struct{id: str, gq: int32, cn: int32}')))
    return mt3.rows()


def main():
    hl.init()
    # hl.import_vcf('vcf/sv.vcf.gz', force=True, reference_genome='GRCh38').write('vcf/svs.mt', overwrite=True)
    mt = hl.read_matrix_table('vcf/svs.mt')
    rows = sub_setting_mt('R0332_cmg_estonia_wgs', mt)
    logger.info('Variant counts: {}'.format(rows.count()))

    kwargs = {
        'contig': hl.int(rows.locus.contig.split('chr')[1]),
        'sc': rows.info.AC,
        'sf': rows.info.AF,
        'sn': rows.info.AN,
        'svDetail': hl.if_else(hl.is_defined(rows.info.CPX_TYPE), rows.info.CPX_TYPE, rows.info.SVTYPE),
        'start': rows.locus.position,
        'end': rows.info.END,
        'sv_callset_Hemi': rows.info.N_HET,
        'sv_callset_Hom': rows.info.N_HOMALT,
        'gnomad_svs_ID': rows.info.gnomAD_V2_SVID,
        'gnomad_svs_AF': rows.info.gnomAD_V2_AF,
        'chr2': rows.info.CHR2,
        'end2': rows.info.END2,
        GENES_FIELD: hl.flatten(
            [hl.if_else(hl.is_defined(rows.info[field_name]), rows.info[field_name], hl.empty_array('str'))
             for field_name in GENES_COLUMNS]),
    }
    rows = rows.annotate(**kwargs)

    mapping = {
        'rsid': 'variantId',
        'alleles': 'svType',
        # 'filters': 'filters',
    }
    rows = rows.rename(mapping)

    fields = list(mapping.values()) + list(kwargs.keys()) + ['samples']
    rows = rows.key_by('locus').select(*fields)

    rows.show(width=200)

    logger.info('Variants: {}'.format(rows.count()))

if __name__ == '__main__':
    main()

# Outputs:
# 2021-03-22 11:43:21 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# Setting default log level to "WARN".
# To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
# 2021-03-22 11:43:22 WARN  Hail:37 - This Hail JAR was compiled for Spark 2.4.5, running with Spark 2.4.1.
#   Compatibility is not guaranteed.
# 2021-03-22 11:43:23 WARN  Utils:66 - Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
# Running on Apache Spark version 2.4.1
# SparkUI available at http://wm598-921.fios-router.home:4041
# Welcome to
#      __  __     <>__
#     / /_/ /__  __/ /
#    / __  / _ `/ / /
#   /_/ /_/\_,_/_/_/   version 0.2.61-3c86d3ba497a
# LOGGING: writing to /Users/shifa/dev/hail_elasticsearch_pipelines/genome_sv_pipeline/hail-20210322-1143-0.2.61-3c86d3ba497a.log
# [Stage 0:> (0 + 1) / 1]INFO:__main__:Data counts: (67889, 106)
# [Stage 1:> (0 + 1) / 1]INFO:__main__:{'DEL': 24207, 'CPX': 36, 'CNV': 526, 'INS': 99, 'DUP': 24512}
