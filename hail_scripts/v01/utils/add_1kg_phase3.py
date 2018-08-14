import hail
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

G1k_VDS_PATHS = {
    '37': 'gs://seqr-reference-data/GRCh37/1kg/1kg.wgs.phase3.20130502.GRCh37_sites.vds',
    '38': 'gs://seqr-reference-data/GRCh38/1kg/1kg.wgs.phase3.20170504.GRCh38_sites.vds',
}
G1K_FIELDS = """
    AC: Int,
    AF: Float,
    AN: Int,
    --- EAS_AF: Float,
    --- EUR_AF: Float,
    --- AFR_AF: Float,
    --- AMR_AF: Float,
    --- SAS_AF: Float,
    POPMAX_AF: Float,
"""


def read_1kg_phase3_vds(hail_context, genome_version, subset=None):
    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    g1k_vds = hail_context.read(G1k_VDS_PATHS[genome_version]).split_multi()

    if subset:
        g1k_vds = g1k_vds.filter_intervals(hail.Interval.parse(subset))

    return g1k_vds


def add_1kg_phase3_to_vds(hail_context, vds, genome_version, root="va.g1k", fields=G1K_FIELDS, subset=None, verbose=True):
    """Add 1000 genome AC and AF annotations to the vds"""

    g1k_vds = read_1kg_phase3_vds(hail_context, genome_version, subset=subset)

    expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=fields,
        other_source_root="vds.info",
    )

    if verbose:
        print(expr)
        #print("\n==> 1kg summary: ")
        #print("\n" + str(g1k_vds.summarize()))

    return vds.annotate_variants_vds(g1k_vds, expr=expr)
