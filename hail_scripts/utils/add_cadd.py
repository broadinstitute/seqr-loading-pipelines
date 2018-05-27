import logging

from hail_scripts.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

logger = logging.getLogger()

CADD_FIELDS = """
    PHRED: Double,
    RawScore: Double,
"""


def add_cadd_to_vds(hail_context, vds, genome_version, root="va.cadd", info_fields=CADD_FIELDS, subset=None, verbose=True):
    """Add CADD scores to the vds"""

    if genome_version != "37" and genome_version != "38":
        raise ValueError("Invalid genome_version: " + str(genome_version))

    expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=info_fields,
        other_source_root="vds.info",
    )

    if verbose:
        print(expr)
        #print("\n==> cadd summary: ")
        #print("\n" + str(cadd_vds.summarize()))

    cadd_vds_path = "gs://seqr-reference-data/GRCh%(genome_version)s/CADD/CADD_snvs_and_indels.vds" % locals()

    logger.info("==> Reading in CADD: %s" % cadd_vds_path)
    cadd_vds = hail_context.read(cadd_vds_path)

    if subset:
        import hail
        cadd_vds = cadd_vds.filter_intervals(hail.Interval.parse(subset))

    vds = vds.annotate_variants_vds(cadd_vds, expr=expr)

    return vds