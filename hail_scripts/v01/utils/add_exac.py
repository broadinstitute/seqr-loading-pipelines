import hail
from pprint import pprint
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

EXAC_VDS_PATHS = {
    '37': 'gs://seqr-reference-data/GRCh37/gnomad/ExAC.r1.sites.vds',
    '38': 'gs://seqr-reference-data/GRCh38/gnomad/ExAC.r1.sites.liftover.b38.vds',
}

ALL_TOP_LEVEL_FIELDS = """
    rsid: String,
    qual: Double,
    filters: Set[String],
"""

ALL_INFO_FIELDS = """
    AC: Array[Int],
    AC_AFR: Array[Int],
    AC_AMR: Array[Int],
    AC_Adj: Array[Int],
    AC_EAS: Array[Int],
    AC_FIN: Array[Int],
    AC_Hemi: Array[Int],
    AC_Het: Array[Int],
    AC_Hom: Array[Int],
    AC_NFE: Array[Int],
    AC_OTH: Array[Int],
    AC_SAS: Array[Int],
    AF: Array[Double],
    AN: Int,
    AN_AFR: Int,
    AN_AMR: Int,
    AN_Adj: Int,
    AN_EAS: Int,
    AN_FIN: Int,
    AN_NFE: Int,
    AN_OTH: Int,
    AN_SAS: Int,
    AN_AFR: Float,
    AN_AMR: Float,
    AN_EAS: Float,
    AN_FIN: Float,
    AN_NFE: Float,
    AN_OTH: Float,
    AN_SAS: Float,
    BaseQRankSum: Double,
    ClippingRankSum: Double,
    DP: Int,
    DS: Boolean,
    FS: Double,
    GQ_MEAN: Double,
    GQ_STDDEV: Double,
    HWP: Double,
    HaplotypeScore: Double,
    Hemi_AFR: Array[Int],
    Hemi_AMR: Array[Int],
    Hemi_EAS: Array[Int],
    Hemi_FIN: Array[Int],
    Hemi_NFE: Array[Int],
    Hemi_OTH: Array[Int],
    Hemi_SAS: Array[Int],
    Het_AFR: Array[Int],
    Het_AMR: Array[Int],
    Het_EAS: Array[Int],
    Het_FIN: Array[Int],
    Het_NFE: Array[Int],
    Het_OTH: Array[Int],
    Het_SAS: Array[Int],
    Hom_AFR: Array[Int],
    Hom_AMR: Array[Int],
    Hom_EAS: Array[Int],
    Hom_FIN: Array[Int],
    Hom_NFE: Array[Int],
    Hom_OTH: Array[Int],
    Hom_SAS: Array[Int],
    InbreedingCoeff: Double,
    MLEAC: Array[Int],
    MLEAF: Array[Double],
    MQ: Double,
    MQ0: Int,
    MQRankSum: Double,
    QD: Double,
    ReadPosRankSum: Double,
    VQSLOD: Double,
    culprit: String,
    DP_HIST: Array[String],
    GQ_HIST: Array[String],
    DOUBLETON_DIST: Array[String],
    AC_MALE: Array[String],
    AC_FEMALE: Array[String],
    AN_MALE: String,
    AN_FEMALE: String,
    AC_CONSANGUINEOUS: Array[String],
    AN_CONSANGUINEOUS: String,
    Hom_CONSANGUINEOUS: Array[String],
    """



USEFUL_TOP_LEVEL_FIELDS = ""
USEFUL_INFO_FIELDS = """
    --- AC: Array[Int],
    AC_Adj: Array[Int],
    AC_Het: Array[Int],
    AC_Hom: Array[Int],
    AC_Hemi: Array[Int],
    --- AN: Int,
    AN_Adj: Int,
    AF: Array[Double],
    --- AC_AFR: Array[Int],
    --- AC_AMR: Array[Int],
    --- AC_EAS: Array[Int],
    --- AC_FIN: Array[Int],
    --- AC_NFE: Array[Int],
    --- AC_OTH: Array[Int],
    --- AC_SAS: Array[Int],
    --- AF_AFR: Float,
    --- AF_AMR: Float,
    --- AF_EAS: Float,
    --- AF_FIN: Float,
    --- AF_NFE: Float,
    --- AF_OTH: Float,
    --- AF_SAS: Float,
    AF_POPMAX: Float,
    --- POPMAX: Array[String],
"""

def read_exac_vds(hail_context, genome_version, subset=None):
    if genome_version in ["37", "38"]:
        exac_vds_path = EXAC_VDS_PATHS[genome_version]
    else:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    exac_vds = hail_context.read(exac_vds_path).split_multi()

    if subset:
        exac_vds = exac_vds.filter_intervals(hail.Interval.parse(subset))

    return exac_vds


def add_exac_to_vds(hail_context, vds, genome_version, root="va.exac", top_level_fields=USEFUL_TOP_LEVEL_FIELDS, info_fields=USEFUL_INFO_FIELDS, subset=None, verbose=True):

    exac_vds = read_exac_vds(hail_context, genome_version, subset=subset)

    # ExAC VCF doesn't contain AF fields, so compute them here
    exac_vds = exac_vds.annotate_variants_expr("""
          va.info.AF_AFR = if(va.info.AN_AFR > 0) va.info.AC_AFR[va.aIndex-1] / va.info.AN_AFR else NA:Float,
          va.info.AF_AMR = if(va.info.AN_AMR > 0) va.info.AC_AMR[va.aIndex-1] / va.info.AN_AMR else NA:Float,
          va.info.AF_EAS = if(va.info.AN_EAS > 0) va.info.AC_EAS[va.aIndex-1] / va.info.AN_EAS else NA:Float,
          va.info.AF_FIN = if(va.info.AN_FIN > 0) va.info.AC_FIN[va.aIndex-1] / va.info.AN_FIN else NA:Float,
          va.info.AF_NFE = if(va.info.AN_NFE > 0) va.info.AC_NFE[va.aIndex-1] / va.info.AN_NFE else NA:Float,
          va.info.AF_OTH = if(va.info.AN_OTH > 0) va.info.AC_OTH[va.aIndex-1] / va.info.AN_OTH else NA:Float,
          va.info.AF_SAS = if(va.info.AN_SAS > 0) va.info.AC_SAS[va.aIndex-1] / va.info.AN_SAS else NA:Float,
          va.info.AF_POPMAX = if(va.info.AN_POPMAX[va.aIndex-1] != "NA" && va.info.AN_POPMAX[va.aIndex-1].toInt() > 0) va.info.AC_POPMAX[va.aIndex-1].toInt() / va.info.AN_POPMAX[va.aIndex-1].toInt() else NA:Float
    """)

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

    expr = []
    if top_fields_expr:
        expr.append(top_fields_expr)
    if info_fields_expr:
        expr.append(info_fields_expr)
    vds = (vds
        .annotate_variants_vds(exac_vds, expr=", ".join(expr))
    )

    if verbose:
        print(info_fields_expr)
        pprint(vds.variant_schema)

    return vds
