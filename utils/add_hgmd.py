from utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

HGMD_INFO_FIELDS = """
    CLASS: String,
    RANKSCORE: Double
"""


def read_hgmd_vds(hail_context, genome_version, subset=None):

    if genome_version == "37":
        hgmd_vds_path = 'gs://seqr-reference-data-private/GRCh37/HGMD/HGMD_PRO_2018.1_hg19_without_DB_field.vds'
    elif genome_version == "38":
        hgmd_vds_path = 'gs://seqr-reference-data-private/GRCh38/HGMD/HGMD_PRO_2018.1_hg38_without_DB_field.vds'
    else:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    hgmd_vds = hail_context.read(hgmd_vds_path).split_multi()

    if subset:
        import hail
        hgmd_vds = hgmd_vds.filter_intervals(hail.Interval.parse(subset))

    return hgmd_vds


def add_hgmd_to_vds(hail_context, vds, genome_version, root="va.hgmd", subset=None, verbose=True):

    hgmd_vds = read_hgmd_vds(hail_context, genome_version, subset=subset)

    vds = vds.annotate_variants_vds(hgmd_vds, expr="""
        %(root)s.class = vds.info.CLASS,
        %(root)s.rankscore = vds.info.RANKSCORE
    """ % locals())

    return vds
