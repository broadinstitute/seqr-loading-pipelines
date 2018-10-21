import argparse as ap
import hail
import logging
from pprint import pprint


from hail_scripts.v01.utils.add_gnomad import GNOMAD_SEQR_VDS_PATHS
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr
from hail_scripts.v01.utils.vds_utils import write_vds, read_vds

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = ap.ArgumentParser()
p.add_argument("--genome-version", help="Genome build: 37 or 38", choices=["37", "38"])
p.add_argument("--exomes-or-genomes", help="Exomes or genomes", choices=["exomes", "genomes"])
p.add_argument("--start-with-step", help="Whether to start at an intermediate step in the pipeline", type=int, default=0, choices=list(range(4)))
args = p.parse_args()

hc = hail.HailContext(log="hail.log")

GNOMAD_SOURCE_VDS_PATHS = {
    "exomes_37": "gs://gnomad-public/release/2.0.2/vds/exomes/gnomad.exomes.r2.0.2.sites.vds",
    "exomes_38": "gs://gnomad-public/release/2.0.2/vcf/exomes/liftover_grch38/gnomad.exomes.r2.0.2.sites.liftover.b38.vcf.gz",
    "genomes_37": "gs://gnomad-public/release/2.0.2/vds/genomes/gnomad.genomes.r2.0.2.sites.vds",
    "genomes_38": "gs://gnomad-public/release/2.0.2/vcf/genomes/liftover_grch38/gnomad.genomes.r2.0.2.sites.liftover.b38.autosomes_and_X.vcf.gz",
}

ALL_TOP_LEVEL_FIELDS = """
    rsid: String,
    qual: Double,
    filters: Set[String],
    pass: Boolean
"""

ALL_INFO_FIELDS = """
    AC: Array[Int],
    AF: Array[Double],
    AN: Int,
    BaseQRankSum: Double,
    ClippingRankSum: Double,
    DB: Boolean,
    DP: Int,
    FS: Double,
    InbreedingCoeff: Double,
    MQ: Double,
    MQRankSum: Double,
    QD: Double,
    ReadPosRankSum: Double,
    SOR: Double,
    VQSLOD: Double,
    VQSR_culprit: String,
    VQSR_NEGATIVE_TRAIN_SITE: Boolean,
    VQSR_POSITIVE_TRAIN_SITE: Boolean,
    GQ_HIST_ALT: Array[String],
    DP_HIST_ALT: Array[String],
    AB_HIST_ALT: Array[String],
    GQ_HIST_ALL: String,
    DP_HIST_ALL: String,
    AB_HIST_ALL: String,
    AC_AFR: Array[Int],
    AC_AMR: Array[Int],
    AC_ASJ: Array[Int],
    AC_EAS: Array[Int],
    AC_FIN: Array[Int],
    AC_NFE: Array[Int],
    AC_OTH: Array[Int],
    AC_SAS: Array[Int],
    AC_Male: Array[Int],
    AC_Female: Array[Int],
    AN_AFR: Int,
    AN_AMR: Int,
    AN_ASJ: Int,
    AN_EAS: Int,
    AN_FIN: Int,
    AN_NFE: Int,
    AN_OTH: Int,
    AN_SAS: Int,
    AN_Male: Int,
    AN_Female: Int,
    AF_AFR: Array[Double],
    AF_AMR: Array[Double],
    AF_ASJ: Array[Double],
    AF_EAS: Array[Double],
    AF_FIN: Array[Double],
    AF_NFE: Array[Double],
    AF_OTH: Array[Double],
    AF_SAS: Array[Double],
    AF_Male: Array[Double],
    AF_Female: Array[Double],
    GC_AFR: Array[Int],
    GC_AMR: Array[Int],
    GC_ASJ: Array[Int],
    GC_EAS: Array[Int],
    GC_FIN: Array[Int],
    GC_NFE: Array[Int],
    GC_OTH: Array[Int],
    GC_SAS: Array[Int],
    GC_Male: Array[Int],
    GC_Female: Array[Int],
    AC_raw: Array[Int],
    AN_raw: Int,
    AF_raw: Array[Double],
    GC_raw: Array[Int],
    GC: Array[Int],
    Hom_AFR: Array[Int],
    Hom_AMR: Array[Int],
    Hom_ASJ: Array[Int],
    Hom_EAS: Array[Int],
    Hom_FIN: Array[Int],
    Hom_NFE: Array[Int],
    Hom_OTH: Array[Int],
    Hom_SAS: Array[Int],
    Hom_Male: Array[Int],
    Hom_Female: Array[Int],
    Hom_raw: Array[Int],
    Hom: Array[Int],
    STAR_AC: Int,
    STAR_AC_raw: Int,
    STAR_Hom: Int,
    POPMAX: Array[String],
    AC_POPMAX: Array[Int],
    AN_POPMAX: Array[Int],
    AF_POPMAX: Array[Double],
    DP_MEDIAN: Array[Int],
    DREF_MEDIAN: Array[Double],
    GQ_MEDIAN: Array[Int],
    AB_MEDIAN: Array[Double],
    AS_RF: Array[Double],
    AS_FilterStatus: Array[Set[String]],
    AS_RF_POSITIVE_TRAIN: Array[Int],
    AS_RF_NEGATIVE_TRAIN: Array[Int],
    CSQ: Array[String],
    AN_FIN_Male: Int,
    AN_EAS_Female: Int,
    AN_NFE_Female: Int,
    AC_AFR_Male: Array[Int],
    AN_AMR_Female: Int,
    AF_AMR_Male: Array[Double],
    Hemi_NFE: Array[Int],
    Hemi_AFR: Array[Int],
    AC_ASJ_Female: Array[Int],
    AF_FIN_Female: Array[Double],
    AN_ASJ_Male: Int,
    AC_OTH_Female: Array[Int],
    GC_OTH_Male: Array[Int],
    GC_FIN_Male: Array[Int],
    AC_NFE_Female: Array[Int],
    AC_EAS_Male: Array[Int],
    AC_OTH_Male: Array[Int],
    GC_SAS_Male: Array[Int],
    Hemi_AMR: Array[Int],
    AC_NFE_Male: Array[Int],
    Hemi: Array[Int],
    AN_FIN_Female: Int,
    GC_EAS_Male: Array[Int],
    GC_ASJ_Female: Array[Int],
    GC_SAS_Female: Array[Int],
    GC_ASJ_Male: Array[Int],
    Hemi_SAS: Array[Int],
    AN_ASJ_Female: Int,
    AF_FIN_Male: Array[Double],
    AN_OTH_Male: Int,
    AF_AFR_Male: Array[Double],
    STAR_Hemi: Int,
    AF_SAS_Male: Array[Double],
    Hemi_ASJ: Array[Int],
    AN_SAS_Female: Int,
    AN_AFR_Female: Int,
    Hemi_raw: Array[Int],
    AF_OTH_Male: Array[Double],
    AC_SAS_Female: Array[Int],
    AF_NFE_Female: Array[Double],
    AF_EAS_Female: Array[Double],
    AN_OTH_Female: Int,
    AF_EAS_Male: Array[Double],
    AF_SAS_Female: Array[Double],
    GC_AFR_Female: Array[Int],
    AF_AFR_Female: Array[Double],
    AC_FIN_Female: Array[Int],
    Hemi_OTH: Array[Int],
    GC_AMR_Male: Array[Int],
    AC_AFR_Female: Array[Int],
    GC_NFE_Male: Array[Int],
    AF_AMR_Female: Array[Double],
    GC_NFE_Female: Array[Int],
    AN_AFR_Male: Int,
    AN_NFE_Male: Int,
    AC_AMR_Male: Array[Int],
    GC_AMR_Female: Array[Int],
    AC_SAS_Male: Array[Int],
    AF_ASJ_Male: Array[Double],
    GC_FIN_Female: Array[Int],
    AC_EAS_Female: Array[Int],
    AC_AMR_Female: Array[Int],
    Hemi_FIN: Array[Int],
    AC_FIN_Male: Array[Int],
    GC_EAS_Female: Array[Int],
    AF_ASJ_Female: Array[Double],
    AF_OTH_Female: Array[Double],
    GC_AFR_Male: Array[Int],
    AN_SAS_Male: Int,
    AF_NFE_Male: Array[Double],
    AN_EAS_Male: Int,
    AC_ASJ_Male: Array[Int],
    Hemi_EAS: Array[Int],
    AN_AMR_Male: Int,
    GC_OTH_Female: Array[Int]
"""


USEFUL_TOP_LEVEL_FIELDS = ""
USEFUL_INFO_FIELDS = """
    AC: Array[Int],
    Hom: Array[Int],
    Hemi: Array[Int],
    AF: Array[Double],
    AN: Int,
    --- AC_AFR: Array[Int],
    --- AC_AMR: Array[Int],
    --- AC_ASJ: Array[Int],
    --- AC_EAS: Array[Int],
    --- AC_FIN: Array[Int],
    --- AC_NFE: Array[Int],
    --- AC_OTH: Array[Int],
    --- AC_SAS: Array[Int],
    --- AF_AFR: Array[Double],
    --- AF_AMR: Array[Double],
    --- AF_ASJ: Array[Double],
    --- AF_EAS: Array[Double],
    --- AF_FIN: Array[Double],
    --- AF_NFE: Array[Double],
    --- AF_OTH: Array[Double],
    --- AF_SAS: Array[Double],
    --- AF_POPMAX: Array[Double],
    --- POPMAX: Array[String],
    AF_POPMAX_OR_GLOBAL: Double
"""

genome_versions = ["37", "38"]
if args.genome_version:
    genome_versions = [args.genome_version]

exomes_genomes_list = ["exomes", "genomes"]
if args.exomes_or_genomes:
    exomes_genomes_list = [args.exomes_or_genomes]


for i, exomes_or_genomes in enumerate(exomes_genomes_list):
    for j, genome_version in enumerate(genome_versions):
        pipeline_step = i*2 + j
        if args.start_with_step > pipeline_step:
            continue

        label = "{}_{}".format(exomes_or_genomes, genome_version)
        source_file = GNOMAD_SOURCE_VDS_PATHS[label]
        logger.info("==> Step {}: {} - generating {}".format(pipeline_step, label, source_file))

        if not isinstance(source_file, list) and source_file.endswith(".vds"):
            vds = read_vds(hc, source_file)
        else:
            vds = hc.import_vcf(source_file, force_bgz=True, min_partitions=10000, drop_samples=True)

        vds = vds.split_multi()

        pprint(vds.variant_schema)

        # add AF_POPMAX_OR_GLOBAL field which improves on AF_POPMAX by only including large sub-populations,
        # and falling back on global AF when AN < 2000 for all sub-populations.
        subpoulations = ["AFR", "AMR", "EAS", "NFE"]
        if exomes_or_genomes == "exomes":
            subpoulations.append("SAS")  # only gnomad exomes have SAS defined

        subpopulation_exprs = ", ".join([
            "if(va.info.AN_{subpop} > 2000) va.info.AF_{subpop}[va.aIndex-1] else NA:Double".format(**locals()) for subpop in subpoulations
        ])

        vds = vds.annotate_variants_expr(
            "va.info.AF_POPMAX_OR_GLOBAL = [ va.info.AF[va.aIndex-1], {subpopulation_exprs} ].max()".format(**locals())
        )

        top_fields_expr = convert_vds_schema_string_to_annotate_variants_expr(
            root="va.clean",
            other_source_fields=USEFUL_TOP_LEVEL_FIELDS,
            other_source_root="va",
        )
        info_fields_expr = convert_vds_schema_string_to_annotate_variants_expr(
            root="va.clean.info",
            other_source_fields=USEFUL_INFO_FIELDS,
            other_source_root="va.info",
        )

        expr = []
        if top_fields_expr:
            expr.append(top_fields_expr)
        if info_fields_expr:
            expr.append(info_fields_expr)

        vds = vds.annotate_variants_expr(expr=expr)
        vds = vds.annotate_variants_expr("va = va.clean")

        pprint(vds.variant_schema)

        write_vds(vds, GNOMAD_SEQR_VDS_PATHS[label])
