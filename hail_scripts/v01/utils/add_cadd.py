import hail
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

CADD_FIELDS = """
    PHRED: Double,
    --- RawScore: Double,
"""

CADD_VDS_PATHS = {
    '37': 'gs://seqr-reference-data/GRCh37/CADD/CADD_snvs_and_indels.v1.4.vds',
    '38': 'gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.4.vds',
}


def read_cadd_vds(hail_context, genome_version, subset=None):
    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    cadd_vds = hail_context.read(CADD_VDS_PATHS[genome_version]).split_multi()

    if subset:
        cadd_vds = cadd_vds.filter_intervals(hail.Interval.parse(subset))

    return cadd_vds


def add_cadd_to_vds(hail_context, vds, genome_version, root="va.cadd", info_fields=CADD_FIELDS, subset=None, verbose=True):
    """Add CADD scores to the vds"""

    expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=info_fields,
        other_source_root="vds.info",
    )

    if verbose:
        print(expr)
        #print("\n==> cadd summary: ")
        #print("\n" + str(cadd_vds.summarize()))

    cadd_vds = read_cadd_vds(hail_context, genome_version, subset=subset)

    vds = vds.annotate_variants_vds(cadd_vds, expr=expr)

    return vds
