import hail
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

DBNSFP_VDS_PATHS = {
    "37": "gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9.3_variant.vds",
    "38": "gs://seqr-reference-data/GRCh38/dbNSFP/v3.5/dbNSFP3.5a_variant.vds",
}

DBNSFP_SCHEMA_37 = """
     --- chr: String,
     --- pos: Int,
     --- ref: String,
     --- alt: String,
     SIFT_pred: String,
     --- Polyphen2_HDIV_pred: String,
     Polyphen2_HVAR_pred: String,
     --- LRT_pred: String,
     MutationTaster_pred: String,
     --- MutationAssessor_pred: String,
     FATHMM_pred: String,
     MetaSVM_pred: String,
     --- MetaLR_pred: String,
     --- VEST3_score: String,
     --- VEST3_rankscore: String,
     --- PROVEAN_pred: String,
     --- M_CAP_pred: String,
     REVEL_score: String,
     --- REVEL_rankscore: String,
     --- MutPred_Top5features: String,
     --- Eigen_phred: String,
     --- Eigen_PC_phred: String,
     GERP_RS: String,
     --- GERP_RS_rankscore: String,
     --- phyloP46way_primate: String,
     --- phyloP46way_primate_rankscore: String,
     --- phyloP46way_placental: String,
     --- phyloP46way_placental_rankscore: String,
     --- phyloP100way_vertebrate: String,
     --- phyloP100way_vertebrate_rankscore: String,
     --- phastCons46way_primate: String,
     --- phastCons46way_primate_rankscore: String,
     --- phastCons46way_placental: String,
     --- phastCons46way_placental_rankscore: String,
     phastCons100way_vertebrate: String,
     --- phastCons100way_vertebrate_rankscore: String,
     --- SiPhy_29way_pi: String,
     --- SiPhy_29way_logOdds_rankscore: String,
     --- ESP6500_AA_AF: Float,
     --- ESP6500_EA_AF: String,
     --- ARIC5606_AA_AC: String,
     --- ARIC5606_AA_AF: String,
     --- ARIC5606_EA_AC: String,
     --- ARIC5606_EA_AF: String
"""

DBNSFP_SCHEMA_38 = """
     --- chr: String,
     --- pos: Int,
     --- ref: String,
     --- alt: String,
     --- SIFT_score: String,
     SIFT_pred: String,
     --- Polyphen2_HDIV_score: String,
     --- Polyphen2_HVAR_score: String,
     Polyphen2_HVAR_pred: String,
     --- LRT_pred: String,
     MutationTaster_pred: String,
     --- MutationAssessor_pred: String,
     FATHMM_pred: String,
     --- PROVEAN_pred: String,
     --- VEST3_rankscore: String,
     MetaSVM_pred: String,
     --- MetaLR_pred: String,
     --- M_CAP_pred: String,
     REVEL_score: String,
     --- REVEL_rankscore: String,
     --- MutPred_Top5features: String,
     DANN_score: String,
     --- DANN_rankscore: String,
     --- fathmm_MKL_coding_pred: String,
     --- Eigen_phred: String,
     --- Eigen_PC_phred: String,
     --- GenoCanyon_score: String,
     --- GenoCanyon_score_rankscore: String,
     --- integrated_fitCons_score: String,
     --- integrated_fitCons_score_rankscore: String,
     --- integrated_confidence_value: String,
     --- GM12878_fitCons_score: String,
     --- GM12878_fitCons_score_rankscore: String,
     --- GM12878_confidence_value: String,
     --- H1_hESC_fitCons_score: String,
     --- H1_hESC_fitCons_score_rankscore: String,
     --- H1_hESC_confidence_value: String,
     --- HUVEC_fitCons_score: String,
     --- HUVEC_fitCons_score_rankscore: String,
     --- HUVEC_confidence_value: String,
     GERP_RS: String,
     --- GERP_RS_rankscore: String,
     --- phyloP100way_vertebrate: String,
     --- phyloP100way_vertebrate_rankscore: String,
     --- phyloP20way_mammalian: String,
     --- phyloP20way_mammalian_rankscore: String,
     phastCons100way_vertebrate: String,
     --- phastCons100way_vertebrate_rankscore: String,
     --- phastCons20way_mammalian: String,
     --- phastCons20way_mammalian_rankscore: String,
     --- SiPhy_29way_pi: String,
     --- SiPhy_29way_logOdds_rankscore: String,
     --- TWINSUK_AC: Int,
     --- TWINSUK_AF: Float,
     --- ALSPAC_AC: Int,
     --- ALSPAC_AF: Float,
     --- ESP6500_AA_AC: Int,
     --- ESP6500_AA_AF: Float,
     --- ESP6500_EA_AC: Int,
     --- ESP6500_EA_AF: Float,
     --- Interpro_domain: String,
     --- GTEx_V6p_gene: String,
     --- GTEx_V6p_tissue: String,
"""


def read_dbnsfp_vds(hail_context, genome_version, subset=None):
    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    dbnsfp_vds = hail_context.read(DBNSFP_VDS_PATHS[genome_version]).split_multi()

    if subset:
        dbnsfp_vds = dbnsfp_vds.filter_intervals(hail.Interval.parse(subset))

    return dbnsfp_vds


def add_dbnsfp_to_vds(hail_context, vds, genome_version, root="va.dbnsfp", subset=None, verbose=True):
    """Add dbNSFP fields to the VDS"""

    if genome_version == "37":
        dbnsfp_schema = DBNSFP_SCHEMA_37
    elif genome_version == "38":
        dbnsfp_schema = DBNSFP_SCHEMA_38
    else:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=dbnsfp_schema,
        other_source_root="vds",
    )

    if verbose:
        print(expr)

    dbnsfp_vds = read_dbnsfp_vds(hail_context, genome_version, subset=subset)

    return vds.annotate_variants_vds(dbnsfp_vds, expr=expr)
