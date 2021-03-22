import hail as hl
import logging

from sv_pipeline.load_data import get_sample_subset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sub_setting_mt(guid, mt):
    sample_subset = hl.literal(get_sample_subset(guid, 'WGS'))
    mt1 = mt.filter_cols(sample_subset.contains(mt['s']))
    sample_cnt = mt1.count_cols()
    mt2 = mt1.annotate_rows(gt=hl.agg.counter(mt1.GT.is_hom_ref()))
    return mt2.filter_rows(mt2.gt.get(True, 0) < sample_cnt)


def main():
    hl.init()
    # hl.import_vcf('vcf/sv.vcf.gz', force=True, reference_genome='GRCh38').write('vcf/svs.mt', overwrite=True)
    mt = hl.read_matrix_table('vcf/svs.mt')
    mt1 = sub_setting_mt('R0332_cmg_estonia_wgs', mt)
    logger.info('Data counts: {}'.format(mt1.count()))

    # query the svtype's for which RD_CN is not None
    mt4 = mt1.annotate_rows(cnt=hl.agg.counter(hl.is_defined(mt1.RD_CN)))
    mt5 = mt4.filter_rows(mt4.cnt.get(True, 0) > 0)
    rows = mt5.rows()
    counts = rows.aggregate(hl.agg.counter(rows.info.SVTYPE))
    logger.info('{}'.format(counts))

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
