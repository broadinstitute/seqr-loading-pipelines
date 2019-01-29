import hail
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

COMBINED_REFERENCE_DATA_VDS_PATHS = {
    '37': 'gs://seqr-reference-data/GRCh37/all_reference_data/combined_reference_data_grch37.vds',
    '38': 'gs://seqr-reference-data/GRCh38/all_reference_data/combined_reference_data_grch38.vds',
}


COMBINED_REFERENCE_DATA_FIELDS = """
     gnomad_exome_coverage: Double,
     gnomad_genome_coverage: Double,

     cadd.PHRED: Double,
     eigen.Eigen_phred: Double,
     primate_ai.score: Double,
     splice_ai.delta_score: Double,

     dbnsfp.SIFT_pred: String,
     dbnsfp.Polyphen2_HVAR_pred: String,
     dbnsfp.MutationTaster_pred: String,
     dbnsfp.FATHMM_pred: String,
     dbnsfp.MetaSVM_pred: String,
     dbnsfp.REVEL_score: String,
     dbnsfp.DANN_score: String,
     dbnsfp.GERP_RS: String,
     dbnsfp.phastCons100way_vertebrate: String,

     g1k.AC: Int,
     g1k.AF: Double,
     g1k.AN: Int,
     g1k.POPMAX_AF: Double,

     exac.AC_Adj: Int,
     exac.AC_Het: Int,
     exac.AC_Hom: Int,
     exac.AC_Hemi: Int,
     exac.AN_Adj: Int,
     exac.AF: Double,
     exac.AF_POPMAX: Double,

     mpc.MPC: String,

     gnomad_exomes.AC: Int,
     gnomad_exomes.Hom: Int,
     gnomad_exomes.Hemi: Int,
     gnomad_exomes.AF: Double,
     gnomad_exomes.AN: Int,
     gnomad_exomes.AF_POPMAX_OR_GLOBAL: Double,

     gnomad_genomes.AC: Int,
     gnomad_genomes.Hom: Int,
     gnomad_genomes.Hemi: Int,
     gnomad_genomes.AF: Double,
     gnomad_genomes.AN: Int,
     gnomad_genomes.AF_POPMAX_OR_GLOBAL: Double,

     topmed.AC: Int,
     topmed.Het: Int,
     topmed.Hom: Int,
     topmed.AF: Double,
     topmed.AN: Int,
"""


def read_combined_reference_data_vds(hail_context, genome_version, subset=None):
    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    combined_reference_data_vds = hail_context.read(COMBINED_REFERENCE_DATA_VDS_PATHS[genome_version]).split_multi()

    if subset:
        combined_reference_data_vds = combined_reference_data_vds.filter_intervals(hail.Interval.parse(subset))

    return combined_reference_data_vds


def add_combined_reference_data_to_vds(hail_context, vds, genome_version, fields=COMBINED_REFERENCE_DATA_FIELDS, subset=None, verbose=True):
    """Add combined reference data annotations to the vds"""

    combined_reference_data_vds = read_combined_reference_data_vds(hail_context, genome_version, subset=subset)

    expr = convert_vds_schema_string_to_annotate_variants_expr(
        root="va",
        other_source_fields=fields,
        other_source_root="vds",
    )

    if verbose:
        print(expr)

    return vds.annotate_variants_vds(combined_reference_data_vds, expr=expr)
