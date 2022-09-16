import argparse
import logging

import hail as hl

from download_and_create_reference_datasets.v02.hail_scripts.write_combined_reference_data_ht import (
    join_hts,
)

OUTPUT_PATH = "gs://seqr-reference-data/GRCh38/combined_interval_reference_data/combined_interval_reference_data.ht"

logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level="INFO")
logger = logging.getLogger(__name__)


def run(args):
    hl.init(default_reference="GRCh38")
    logger.info("Joining the interval reference datasets")
    joined_ht = join_hts(
        ["gnomad_non_coding_constraint", "screen"], reference_genome="38"
    )

    output_path = args.output_path if args.output_path else OUTPUT_PATH
    logger.info("Writing to %s", output_path)
    joined_ht.write(output_path, overwrite=args.force_write)
    logger.info("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--force-write",
        help="Overwrite an existing output file",
        action="store_true",
    )
    parser.add_argument(
        "-o",
        "--output-path",
        help=f"Output path for the combined reference dataset. Default is {OUTPUT_PATH}",
    )
    args = parser.parse_args()

    run(args)
