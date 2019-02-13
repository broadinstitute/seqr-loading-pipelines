import logging
import os

import hail as hl

CONFIG = {
    '37':  (
            'gs://seqr-reference-data/GRCh37/spliceai/exome_spliceai_scores.vcf.gz',
            'gs://seqr-reference-data/GRCh37/spliceai/whole_genome_filtered_spliceai_scores.vcf.gz'
    ),
    '38': (
            'gs://seqr-reference-data/GRCh38/spliceai/exome_spliceai_scores.liftover.vcf.gz',
            'gs://seqr-reference-data/GRCh38/spliceai/whole_genome_filtered_spliceai_scores.liftover.vcf.gz'
    )
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def vcf_to_mt(splice_ai_snvs_path, splice_ai_indels_path):
    '''
    Loads the snv path and indels source path to a matrix table and returns the table.

    :param splice_ai_snvs_path: source location
    :param splice_ai_indels_path: source location
    :return: matrix table
    '''
    logger.info('==> reading in splice_ai vcfs: %s, %s' % (splice_ai_snvs_path, splice_ai_indels_path))
    mt = hl.import_vcf([splice_ai_snvs_path, splice_ai_indels_path], force_bgz=True, min_partitions=10000)
    mt = hl.filter_intervals(mt, [hl.parse_locus_interval('1-MT')])
    mt = mt.persist()

    # Annotate info.max_DS with the max of DS_AG, DS_AL, DS_DG, DS_DL in info.
    info=mt.info.annotate(max_DS=hl.max([mt.info.DS_AG, mt.info.DS_AL, mt.info.DS_DG, mt.info.DS_DL]))
    mt = mt.annotate_rows(info=info)

    return mt

def run():
    for version, config in CONFIG.items():
        logger.info('===> Version %s' % version)
        mt = vcf_to_mt(config[0], config[1])

        # Write mt to the same directory as the snv source.
        dest = os.path.join(os.path.dirname(CONFIG['37'][0]), "spliceai_scores.mt")
        logger.info('===> Writing to %s' % dest)
        mt.write(dest)
        mt.describe()

run()
