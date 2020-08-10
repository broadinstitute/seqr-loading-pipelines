import logging
import os

import hail as hl

from gnomad.resources.resource_utils import NO_CHR_TO_CHR_CONTIG_RECODING

CONFIG = {
    "37": (
        "gs://seqr-reference-data/GRCh37/spliceai/new-version-2019-10-11/spliceai_scores.masked.snv.hg19.vcf.gz",
        "gs://seqr-reference-data/GRCh37/spliceai/new-version-2019-10-11/spliceai_scores.masked.indel.hg19.vcf.gz",
    ),
    "38": (
        "gs://seqr-reference-data/GRCh38/spliceai/new-version-2019-10-11/spliceai_scores.masked.snv.hg38.vcf.gz",
        "gs://seqr-reference-data/GRCh38/spliceai/new-version-2019-10-11/spliceai_scores.masked.indel.hg38.vcf.gz",
    ),
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def vcf_to_mt(splice_ai_snvs_path, splice_ai_indels_path, genome_version):
    """
    Loads the snv path and indels source path to a matrix table and returns the table.

    :param splice_ai_snvs_path: source location
    :param splice_ai_indels_path: source location
    :return: matrix table
    """

    logger.info(
        "==> reading in splice_ai vcfs: %s, %s"
        % (splice_ai_snvs_path, splice_ai_indels_path)
    )

    # for 37, extract to MT, for 38, MT not included.
    interval = "1-MT" if genome_version == "37" else "chr1-chrY"
    contig_dict = None
    if genome_version == "38":
        contig_dict = NO_CHR_TO_CHR_CONTIG_RECODING

    mt = hl.import_vcf(
        [splice_ai_snvs_path, splice_ai_indels_path],
        reference_genome=f"GRCh{genome_version}",
        contig_recoding=contig_dict,
        force_bgz=True,
        min_partitions=10000,
        skip_invalid_loci=True,
    )
    interval = [
        hl.parse_locus_interval(interval, reference_genome=f"GRCh{genome_version}")
    ]
    mt = hl.filter_intervals(mt, interval)

    # Split SpliceAI field by | delimiter. Capture delta score entries and map to floats
    delta_scores = mt.info.SpliceAI[0].split(delim="\\|")[2:6]
    splice_split = mt.info.annotate(
        SpliceAI=hl.map(lambda x: hl.float32(x), delta_scores)
    )
    mt = mt.annotate_rows(info=splice_split)

    # Annotate info.max_DS with the max of DS_AG, DS_AL, DS_DG, DS_DL in info.
    # delta_score array is |DS_AG|DS_AL|DS_DG|DS_DL
    consequences = hl.literal(
        ["Acceptor gain", "Acceptor loss", "Donor gain", "Donor loss"]
    )
    mt = mt.annotate_rows(info=mt.info.annotate(max_DS=hl.max(mt.info.SpliceAI)))
    mt = mt.annotate_rows(
        info=mt.info.annotate(
            splice_consequence=hl.if_else(
                mt.info.max_DS > 0,
                consequences[mt.info.SpliceAI.index(mt.info.max_DS)],
                "No consequence",
            )
        )
    )
    return mt


def run():
    for version, config in CONFIG.items():
        logger.info("===> Version %s" % version)
        mt = vcf_to_mt(config[0], config[1], version)

        # Write mt as a ht to the same directory as the snv source.
        dest = os.path.join(os.path.dirname(CONFIG[version][0]), "spliceai_scores_8102020.ht")
        logger.info("===> Writing to %s" % dest)
        ht = mt.rows()
        ht.write(dest)
        ht.describe()


run()
