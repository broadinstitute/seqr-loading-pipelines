import logging

import hail as hl

from hail_scripts.v02.utils.hail_utils import import_vcf

CONFIG= {
    '37': 'gs://seqr-reference-data/GRCh37/gnomad/ExAC.r1.sites.vep.vcf.bgz',
    '38': 'gs://seqr-reference-data/GRCh38/gnomad/ExAC.r1.sites.liftover.b38.vcf.gz',
}

logger = logging.getLogger('v02.hail_scripts.create_exac_ht')


def vcf_to_mt(path, genome_version):
    '''
    Converts exac vcf to mt and adds info.AF_POPMAX annotation.

    :param path: vcf path
    :param genome_version: genome version
    :return:
    '''
    mt = import_vcf(path,
                    genome_version=genome_version,
                    min_partitions=1000)

    # Annotate rows with popmax af if AC_POPMAX is not 'NA'. Value =
    # AC/AN, and null otherwise.
    cond = (mt.info.AC_POPMAX[mt.a_index-1] != "NA")
    value = (hl.float((hl.int(mt.info.AC_POPMAX[mt.a_index - 1]) /
                       hl.int(mt.info.AN_POPMAX[mt.a_index - 1]))))
    alternative = hl.null(hl.tfloat)
    mt = mt.annotate_rows(info=mt.info.annotate(AF_POPMAX=hl.cond(cond, value, alternative)))

    return mt


def run():
   for genome_version, path in CONFIG.items():
       logger.info('reading from input path: %s' % path)
       mt = vcf_to_mt(path, genome_version)

       mt.describe()

       output_path = path.replace(".vcf", "").replace(".gz", "").replace(".bgz", "")\
                         .replace(".*", "").replace("*", "") + ".ht"
       logger.info('writing to output path: %s' % output_path)
       mt.rows().write(output_path)

run()
