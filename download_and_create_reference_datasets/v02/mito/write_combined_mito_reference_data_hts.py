#!/usr/bin/env python3
import argparse
import logging

import hail as hl

from download_and_create_reference_datasets.v02.hail_scripts.write_combined_reference_data_ht import join_hts

OUTPUT_PATH = 'gs://seqr-reference-data/GRCh38/mitochondrial/all_mito_reference_data/combined_reference_data_chrM.ht'

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level='INFO')
logger = logging.getLogger(__name__)


def run(args):
    # If there are out-of-memory error, set the environment variable with the following command
    # $ export PYSPARK_SUBMIT_ARGS="--driver-memory 4G pyspark-shell"
    # "4G" in the environment variable can be bigger if your computer has a larger memory.
    hl.init(default_reference='GRCh38', min_block_size=128, master='local[32]')

    logger.info('Joining the mitochondrial reference datasets')
    joined_ht = join_hts(['gnomad_mito', 'mitomap', 'mitimpact', 'hmtvar', 'helix_mito', 'clinvar_mito', 'dbnsfp_mito'],
                         reference_genome='38')

    logger.info(f'Writing to {OUTPUT_PATH}')
    joined_ht.write(OUTPUT_PATH, overwrite=args.force_write)
    logger.info('Done')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--force-write', help='Force write to an existing output file', action='store_true')
    args = parser.parse_args()

    run(args)
