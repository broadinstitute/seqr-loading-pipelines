import logging

import hail as hl

from hail_scripts.utils.hail_utils import import_table

logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

CONFIG = {"38": "gs://seqr-reference-data/GRCh38/ccREs/GRCh38-ccREs.bed"}


def make_interval_bed_table(ht, reference_genome):
    """
    Remove the extra fields from the input CCRes file and mimic a bed import.

    :param ht: CCRes bed file.
    :return: Hail table that mimics basic bed file table.
    """
    ht = ht.select(
        interval=hl.locus_interval(
            ht["f0"], ht["f1"], ht["f2"], reference_genome, invalid_missing=True
        ),
        target=ht["f5"],
    )
    return ht


def run():
    for genome_version, path in CONFIG.items():
        logger.info("Reading from input path: %s", path)

        ht = import_table(
            path,
            no_header=True,
            types={
                "f0": hl.tstr,
                "f1": hl.tint32,
                "f2": hl.tint32,
                "f3": hl.tstr,
                "f4": hl.tstr,
                "f5": hl.tstr,
            },
        )
        ht = make_interval_bed_table(ht, genome_version)

        ht.describe()

        output_path = path.replace(".bed", "") + ".ht"
        logger.info("Writing to output path: %s", output_path)
        ht.write(output_path, overwrite=True)


run()
