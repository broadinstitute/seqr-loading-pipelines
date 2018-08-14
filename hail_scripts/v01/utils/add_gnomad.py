import hail
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr


GNOMAD_SEQR_VDS_PATHS = {
    "exomes_37": "gs://seqr-reference-data/GRCh37/gnomad/gnomad.exomes.r2.0.2.sites.grch37.split.vds",
    "exomes_38": "gs://seqr-reference-data/GRCh38/gnomad/gnomad.exomes.r2.0.2.sites.liftover_grch38.split.vds",

    "genomes_37": "gs://seqr-reference-data/GRCh37/gnomad/gnomad.genomes.r2.0.2.sites.grch37.split.vds",
    "genomes_38": "gs://seqr-reference-data/GRCh38/gnomad/gnomad.genomes.r2.0.2.sites.liftover_grch38.split.vds",
}


USEFUL_TOP_LEVEL_FIELDS = ""
USEFUL_INFO_FIELDS = """
    AC: Int,
    Hom: Int,
    Hemi: Int,
    AF: Double,
    AN: Int,
    AF_POPMAX_OR_GLOBAL: Double
"""

def read_gnomad_vds(hail_context, genome_version, exomes_or_genomes, subset=None):
    if genome_version not in ("37", "38"):
        raise ValueError("Invalid genome_version: %s. Must be '37' or '38'" % str(genome_version))

    gnomad_vds_path = GNOMAD_SEQR_VDS_PATHS["%s_%s" % (exomes_or_genomes, genome_version)]

    gnomad_vds = hail_context.read(gnomad_vds_path)

    if subset:
        gnomad_vds = gnomad_vds.filter_intervals(hail.Interval.parse(subset))

    return gnomad_vds


def add_gnomad_to_vds(hail_context, vds, genome_version, exomes_or_genomes, root=None, top_level_fields=USEFUL_TOP_LEVEL_FIELDS, info_fields=USEFUL_INFO_FIELDS, subset=None, verbose=True):
    if genome_version not in ("37", "38"):
        raise ValueError("Invalid genome_version: %s. Must be '37' or '38'" % str(genome_version))

    if exomes_or_genomes not in ("exomes", "genomes"):
        raise ValueError("Invalid genome_version: %s. Must be 'exomes' or 'genomes'" % str(genome_version))

    if root is None:
        root = "va.gnomad_%s" % exomes_or_genomes

    gnomad_vds = read_gnomad_vds(hail_context, genome_version, exomes_or_genomes, subset=subset)

    if exomes_or_genomes == "genomes":
        # remove any *SAS* fields from genomes since South Asian population only defined for exomes
        info_fields = "\n".join(field for field in info_fields.split("\n") if "SAS" not in field)

    top_fields_expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=top_level_fields,
        other_source_root="vds",
    )
    if verbose:
        print(top_fields_expr)

    info_fields_expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=info_fields,
        other_source_root="vds.info",
    )
    if verbose:
        print(info_fields_expr)

    expr = []
    if top_fields_expr:
        expr.append(top_fields_expr)
    if info_fields_expr:
        expr.append(info_fields_expr)
    return (vds
        .annotate_variants_vds(gnomad_vds, expr=", ".join(expr))
    )

