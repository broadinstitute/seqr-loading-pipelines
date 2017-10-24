from utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

TOPMED_FIELDS = """
    AC: Array[Int],
    AF: Array[Float],
"""

def add_topmed_to_vds(hail_context, vds, genome_version, root="va.topmed", fields=TOPMED_FIELDS, subset=None, verbose=True):
    """Add 1000 genome AC and AF annotations to the vds"""

    if genome_version == "37":
        raise ValueError("Not yet available")
    elif genome_version == "38":
        topmed_vds_path = 'gs://seqr-reference-data/GRCh38/TopMed/ALL.TOPMed_freeze5_hg38_dbSNP.vds'
    else:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    topmed_vds = hail_context.read(topmed_vds_path).split_multi()

    if subset:
        import hail
        topmed_vds = topmed_vds.filter_intervals(hail.Interval.parse(subset))

    expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=fields,
        other_source_root="vds.info",
    )

    if verbose:
        print(expr)
        #print("\n==> topmed summary: ")
        #print("\n" + str(topmed_vds.summarize()))

    return vds.annotate_variants_vds(topmed_vds, expr=expr)
