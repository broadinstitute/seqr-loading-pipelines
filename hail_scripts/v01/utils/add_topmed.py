import hail
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

TOPMED_VDS_PATHS = {
    '37': 'gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.vds',
    '38': 'gs://seqr-reference-data/GRCh38/TopMed/bravo-dbsnp-all.vds'
}

TOPMED_FIELDS = """
    AC: Array[Int],
    Het: Array[Int],
    Hom: Array[Int],
    AF: Array[Float],
    AN: Int,
    ---    VRT: Int,
    ---    NS: Int,
"""


def read_topmed_vds(hail_context, genome_version, subset=None):
    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    topmed_vds = hail_context.read(TOPMED_VDS_PATHS[genome_version]).split_multi()

    if subset:
        topmed_vds = topmed_vds.filter_intervals(hail.Interval.parse(subset))

    return topmed_vds


def add_topmed_to_vds(hail_context, vds, genome_version, root="va.topmed", fields=TOPMED_FIELDS, subset=None, verbose=True):
    """Add topmed AC and AF annotations to the vds"""

    expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=fields,
        other_source_root="vds.info",
    )

    if verbose:
        print(expr)

    topmed_vds = read_topmed_vds(hail_context, genome_version, subset=subset)

    return vds.annotate_variants_vds(topmed_vds, expr=expr)
