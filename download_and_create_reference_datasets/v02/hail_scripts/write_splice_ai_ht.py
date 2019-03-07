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

def vcf_to_mt(splice_ai_snvs_path, splice_ai_indels_path, genome_version):
    '''
    Loads the snv path and indels source path to a matrix table and returns the table.

    :param splice_ai_snvs_path: source location
    :param splice_ai_indels_path: source location
    :return: matrix table
    '''

    logger.info('==> reading in splice_ai vcfs: %s, %s' % (splice_ai_snvs_path, splice_ai_indels_path))

    # for 37, extract to MT, for 38, MT not included.
    interval = '1-MT' if genome_version == '37' else 'chr1-chrY'
    contig_dict = None
    if genome_version == '38':
        rg = hl.get_reference('GRCh37')
        grch37_contigs = [x for x in rg.contigs if not x.startswith('GL') and not x.startswith('M')]
        contig_dict = dict(zip(grch37_contigs, ['chr' + x for x in grch37_contigs]))

    mt = hl.import_vcf([splice_ai_snvs_path,
                        splice_ai_indels_path],
                       reference_genome=f"GRCh{genome_version}",
                       contig_recoding=contig_dict,
                       force_bgz=True,
                       min_partitions=10000,
                       skip_invalid_loci=True)
    interval = [hl.parse_locus_interval(interval, reference_genome=f"GRCh{genome_version}")]
    mt = hl.filter_intervals(mt, interval)

    # Annotate info.max_DS with the max of DS_AG, DS_AL, DS_DG, DS_DL in info.
    info=mt.info.annotate(max_DS=hl.max([mt.info.DS_AG, mt.info.DS_AL, mt.info.DS_DG, mt.info.DS_DL]))
    mt = mt.annotate_rows(info=info)

    return mt

def run():
    for version, config in CONFIG.items():
        logger.info('===> Version %s' % version)
        mt = vcf_to_mt(config[0], config[1], version)

        # Write mt as a ht to the same directory as the snv source.
        dest = os.path.join(os.path.dirname(CONFIG[version][0]),
                            "spliceai_scores.ht")
        logger.info('===> Writing to %s' % dest)
        ht = mt.rows()
        ht.write(dest)
        mt.describe()

run()
