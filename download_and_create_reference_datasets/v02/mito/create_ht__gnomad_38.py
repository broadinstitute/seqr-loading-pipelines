#!/usr/bin/env python3

# Download gnomAD mito datset vcf file, import it to a matrix table, annotate required fields into al hail table,
# and save it to a file.

import logging
import hail as hl

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()

CONFIG = {
    'vcf_path': 'gs://gcp-public-data--gnomad/release/3.1/vcf/genomes/gnomad.genomes.v3.1.sites.chrM.vcf.bgz',
    'output_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/gnomad.genomes.v3.1.chrM.ht',
}

MITO_INFO_FIELDS = ['AN', 'AC_hom', 'AC_het', 'AF_hom', 'AF_het', 'max_hl']
OUTPUT_FIELDS = ['locus', 'alleles', 'rsid'] + MITO_INFO_FIELDS


def vcf_to_mt(path):
    """
    Converts vcf to mt.

    :param path: vcf path
    :return:
    """
    logger.info(f'Loading VCF file {path}')
    mt = hl.import_vcf(path, reference_genome='GRCh38')

    return mt


def annotate_ht(mt):
    rows = mt.rows()
    info_field_mapping = {f: rows.info[f] for f in MITO_INFO_FIELDS}
    rows = rows.annotate(**info_field_mapping)
    return rows.key_by().select(*OUTPUT_FIELDS)


def run():
    hl.init()

    mt = vcf_to_mt(CONFIG['vcf_path'])
    ht = annotate_ht(mt)
    logger.info(f'Writing to Hail table {CONFIG["output_path"]}')
    ht.write(CONFIG['output_path'], overwrite=True)
    logger.info('Done')


run()
