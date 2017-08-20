#!/usr/bin/env python

import argparse
import hail
import logging
from pprint import pprint

from utils.computed_fields_utils import get_expr_for_variant_id, \
    get_expr_for_vep_gene_ids_set, get_expr_for_vep_transcript_ids_set, \
    get_expr_for_orig_alt_alleles_set, get_expr_for_vep_consequence_terms_set, \
    get_expr_for_vep_sorted_transcript_consequences_array, \
    get_expr_for_worst_transcript_consequence_annotations_struct, get_expr_for_end_pos, \
    get_expr_for_xpos, get_expr_for_contig, get_expr_for_start_pos, get_expr_for_alt_allele, \
    get_expr_for_ref_allele
from utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr
from utils.add_1kg_phase3 import add_1kg_phase3_to_vds
from utils.add_cadd import add_cadd_to_vds
from utils.add_clinvar import add_clinvar_to_vds
from utils.add_exac import add_exac_to_vds
from utils.add_gnomad import add_gnomad_to_vds
from utils.add_mpc import add_mpc_to_vds
from utils.computed_fields_utils import CONSEQUENCE_TERM_RANKS, CONSEQUENCE_TERMS
import sys

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


p = argparse.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)
p.add_argument("--only-coding", action="store_true")
p.add_argument("--only-non-coding", action="store_true")
p.add_argument("--exclude-1kg", action="store_true", help="Don't add 1kg scores. This is mainly useful for testing.")
p.add_argument("--exclude-cadd", action="store_true", help="Don't add CADD scores (they take a really long time to load). This is mainly useful for testing.")
p.add_argument("--exclude-gnomad", action="store_true", help="Don't add gnomAD exome or genome fields. This is mainly useful for testing.")
p.add_argument("--exclude-exac", action="store_true", help="Don't add ExAC fields. This is mainly useful for testing.")
p.add_argument("input_vds", help="input VDS")

# parse args
args = p.parse_args()

input_vds_path = str(args.input_vds)
if not input_vds_path.endswith(".vds"):
    p.error("Input must be a .vds")
if args.only_coding and args.only_non_coding:
    p.error("Both --only-coding and --only-non-coding options used")

input_vds_path_prefix = input_vds_path.replace(".vds", "")
output_vds_path = input_vds_path_prefix 
if args.only_coding:
    output_vds_path += ".coding"
if args.only_non_coding:
    output_vds_path += ".non_coding"

output_vds_path += ".all_annotations.vds"


logger.info("Input: " + input_vds_path)
logger.info("Output: " + output_vds_path)

logger.info("\n==> create HailContext")
hc = hail.HailContext(log="/hail.log")

logger.info("\n==> import vds: " + input_vds_path)
vds = hc.read(input_vds_path)

#vds.export_variants("gs://seqr-hail/temp/" + os.path.basename(output_vds_path).replace(".vds", "") + ".tsv", "variant = v, va.*")

logger.info("\n==> adding other annotations")

vds_computed_annotation_exprs = [
    "va.geneIds = %s" % get_expr_for_vep_gene_ids_set(vep_root="va.vep"),
    "va.transcriptIds = %s" % get_expr_for_vep_transcript_ids_set(vep_root="va.vep"),
    "va.transcriptConsequenceTerms = %s" % get_expr_for_vep_consequence_terms_set(vep_root="va.vep"),
    "va.sortedTranscriptConsequences = %s" % get_expr_for_vep_sorted_transcript_consequences_array(vep_root="va.vep", include_coding_annotations=not args.only_non_coding),
    "va.mainTranscript = %s" % get_expr_for_worst_transcript_consequence_annotations_struct("va.sortedTranscriptConsequences", include_coding_annotations=not args.only_non_coding),
    "va.sortedTranscriptConsequences = json(va.sortedTranscriptConsequences)",

    "va.variantId = %s" % get_expr_for_variant_id(),

    "va.contig = %s" % get_expr_for_contig(),
    "va.start = %s" % get_expr_for_start_pos(),
    "va.pos = %s" % get_expr_for_start_pos(),
    "va.end = %s" % get_expr_for_end_pos(),
    "va.ref = %s" % get_expr_for_ref_allele(),
    "va.alt = %s" % get_expr_for_alt_allele(),

    "va.xpos = %s" % get_expr_for_xpos(pos_field="start"),
    "va.xstart = %s" % get_expr_for_xpos(pos_field="start"),
    "va.xstop = %s" % get_expr_for_xpos(field_prefix="va.", pos_field="end"),
]

for expr in vds_computed_annotation_exprs:
    vds = vds.annotate_variants_expr(expr)

# filter to coding or non-coding variants
non_coding_consequence_first_index = CONSEQUENCE_TERMS.index("5_prime_UTR_variant")
if args.only_coding:
    logger.info("\n==> filter to coding variants only (all transcript consequences above 5_prime_UTR_variant)")
    vds = vds.filter_variants_expr("va.mainTranscript.major_consequence_rank < %d" % non_coding_consequence_first_index, keep=True)
elif args.only_non_coding:
    logger.info("\n==> filter to non-coding variants only (all transcript consequences above 5_prime_UTR_variant)")
    vds = vds.filter_variants_expr("isMissing(va.mainTranscript.major_consequence_rank) || va.mainTranscript.major_consequence_rank >= %d" % non_coding_consequence_first_index, keep=True)


# apply schema to dataset
INPUT_SCHEMA = {
    "top_level_fields": """
        contig: String,
        start: Int,
        pos: Int,
        end: Int,
        ref: String,
        alt: String,

        xpos: Long,
        xstart: Long,
        xstop: Long,

        rsid: String,
        qual: Double,
        filters: Set[String],
        wasSplit: Boolean,
        aIndex: Int,

        variantId: String,
        originalAltAlleles: Set[String],
        geneIds: Set[String],
        transcriptIds: Set[String],
        transcriptConsequenceTerms: Set[String],
        mainTranscript: Struct,
        sortedTranscriptConsequences: String,
    """,
    "info_fields": """
         AC: Array[Int],
         AF: Array[Double],
         AN: Int,
         --- BaseQRankSum: Double,
         --- ClippingRankSum: Double,
         DP: Int,
         FS: Double,
         InbreedingCoeff: Double,
         MQ: Double,
         --- MQRankSum: Double,
         QD: Double,
         --- ReadPosRankSum: Double,
         VQSLOD: Double,
         culprit: String,
    """
}

expr = convert_vds_schema_string_to_annotate_variants_expr(root="va.clean", **INPUT_SCHEMA)
logger.info(expr)
vds = vds.annotate_variants_expr(expr=expr)
vds = vds.annotate_variants_expr("va = va.clean")

# add reference data
CLINVAR_INFO_FIELDS = """
    MEASURESET_TYPE: String,
    MEASURESET_ID: String,
    RCV: String,
    ALLELE_ID: String,
    CLINICAL_SIGNIFICANCE: String,
    PATHOGENIC: String,
    BENIGN: String,
    CONFLICTED: String,
    REVIEW_STATUS: String,
    GOLD_STARS: String,
    ALL_SUBMITTERS: String,
    ALL_TRAITS: String,
    ALL_PMIDS: String,
    INHERITANCE_MODES: String,
    AGE_OF_ONSET: String,
    PREVALENCE: String,
    DISEASE_MECHANISM: String,
    ORIGIN: String,
    XREFS: String
"""

CADD_INFO_FIELDS = """
    PHRED: Double,
    RawScore: Double,
"""

MPC_INFO_FIELDS = """
    MPC: Double,
    fitted_score: Double,
    mis_badness: Double,
    obs_exp: Double,
"""

EXAC_TOP_LEVEL_FIELDS = """filters: Set[String],"""
EXAC_INFO_FIELDS = """
    AC: Array[Int],
    AC_Adj: Array[Int],
    AN: Int,
    AN_Adj: Int,
    AC_AFR: Array[Int],
    AC_AMR: Array[Int],
    AC_EAS: Array[Int],
    AC_FIN: Array[Int],
    AC_NFE: Array[Int],
    AC_OTH: Array[Int],
    AC_SAS: Array[Int],
    AF_AFR: Float,
    AF_AMR: Float,
    AF_EAS: Float,
    AF_FIN: Float,
    AF_NFE: Float,
    AF_OTH: Float,
    AF_SAS: Float,
    --- AF_POPMAX: Float,
    """

GNOMAD_TOP_LEVEL_FIELDS = """filters: Set[String],"""
GNOMAD_INFO_FIELDS = """
    AC: Array[Int],
    AF: Array[Double],
    AN: Int,
    AC_AFR: Array[Int],
    AC_AMR: Array[Int],
    AC_ASJ: Array[Int],
    AC_EAS: Array[Int],
    AC_FIN: Array[Int],
    AC_NFE: Array[Int],
    AC_OTH: Array[Int],
    AC_SAS: Array[Int],
    AF_AFR: Array[Double],
    AF_AMR: Array[Double],
    AF_ASJ: Array[Double],
    AF_EAS: Array[Double],
    AF_FIN: Array[Double],
    AF_NFE: Array[Double],
    AF_OTH: Array[Double],
    AF_SAS: Array[Double],
    --- POPMAX: Array[String],
    AF_POPMAX: Array[Double],
"""

logger.info("\n==> add clinvar")
vds = add_clinvar_to_vds(hc, vds, args.genome_version, root="va.clinvar", info_fields=CLINVAR_INFO_FIELDS)

if not args.exclude_cadd:
    logger.info("\n==> add cadd")
    vds = add_cadd_to_vds(hc, vds, args.genome_version, root="va.cadd", info_fields=CADD_INFO_FIELDS)

if not args.only_non_coding:
    logger.info("\n==> add mpc")
    vds = add_mpc_to_vds(hc, vds, args.genome_version, root="va.mpc", info_fields=MPC_INFO_FIELDS)

if not args.exclude_1kg:
    logger.info("\n==> add 1kg")
    vds = add_1kg_phase3_to_vds(hc, vds, args.genome_version, root="va.g1k")

if not args.exclude_exac:
    logger.info("\n==> add exac")
    vds = add_exac_to_vds(hc, vds, args.genome_version, root="va.exac", top_level_fields=EXAC_TOP_LEVEL_FIELDS, info_fields=EXAC_INFO_FIELDS)

if not args.exclude_gnomad:
    logger.info("\n==> add gnomad exomes")
    vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="exomes", root="va.gnomad_exomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS)

if not args.exclude_gnomad:
    logger.info("\n==> add gnomad genomes")
    vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="genomes", root="va.gnomad_genomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS)

logger.info("\n==> saving to " + output_vds_path)
vds.write(output_vds_path, overwrite=True)

#vds.export_variants("gs://seqr-hail/temp/" + os.path.basename(output_vds_path).replace(".vds", "") + ".tsv", "variant = v, va.*")

# see https://hail.is/hail/annotationdb.html#query-builder
#vds = vds.annotate_variants_db([
#    'va.cadd.PHRED',
#    'va.cadd.RawScore',
#    'va.dann.score',
#])

pprint(vds.variant_schema)
pprint(vds.summarize())
