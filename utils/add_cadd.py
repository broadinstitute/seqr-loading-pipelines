import logging

from utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

logger = logging.getLogger()

CADD_FIELDS = """
    PHRED: Double,
    RawScore: Double,
"""


def add_cadd_to_vds(hail_context, vds, genome_version, root="va.cadd", info_fields=CADD_FIELDS, subset=None, verbose=True):
    """Add CADD scores to the vds"""

    if genome_version == "37":
        cadd_snvs_vds_path = 'gs://seqr-reference-data/GRCh37/CADD/whole_genome_SNVs.vds'
        cadd_indels_vds_path = 'gs://seqr-reference-data/GRCh37/CADD/InDels.vds'

    elif genome_version == "38":
        cadd_snvs_vds_path = 'gs://seqr-reference-data/GRCh38/CADD/whole_genome_SNVs.liftover.GRCh38.fixed.vds'  #'gs://seqr-reference-data/GRCh38/CADD/whole_genome_SNVs.liftover.GRCh38.vds'
        cadd_indels_vds_path = 'gs://seqr-reference-data/GRCh38/CADD/InDels.liftover.GRCh38.fixed.vds'
    else:
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

    for cadd_vds_path in [cadd_snvs_vds_path, cadd_indels_vds_path]:
        #cadd_vds = hail_context.import_vcf([cadd_snvs_vcf_path, cadd_indels_vcf_path], force_bgz=True, min_partitions=1000)
        #cadd_vds = hail_context.read([cadd_indels_vds_path]).split_multi()

        logger.info("==> Reading in CADD: %s" % cadd_vds_path)
        cadd_vds = hail_context.read(cadd_vds_path).split_multi()

        if subset:
            import hail
            cadd_vds = cadd_vds.filter_intervals(hail.Interval.parse(subset))

        vds = vds.annotate_variants_vds(cadd_vds, expr=expr)

    return vds