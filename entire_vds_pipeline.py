#!/usr/bin/env python
from utils.elasticsearch_utils import export_vds_to_elasticsearch

import argparse
import hail
import logging
import time
from pprint import pprint

from utils.computed_fields_utils import get_expr_for_variant_id, \
    get_expr_for_vep_gene_ids_set, get_expr_for_vep_transcript_ids_set, \
    get_expr_for_orig_alt_alleles_set, get_expr_for_vep_consequence_terms_set, \
    get_expr_for_vep_sorted_transcript_consequences_array, \
    get_expr_for_worst_transcript_consequence_annotations_struct, get_expr_for_end_pos, \
    get_expr_for_xpos, get_expr_for_contig, get_expr_for_start_pos, get_expr_for_alt_allele, \
    get_expr_for_ref_allele
from utils.fam_file_utils import MAX_SAMPLES_PER_INDEX, compute_sample_groups_from_fam_file
from utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr
from utils.add_1kg_phase3 import add_1kg_phase3_to_vds
from utils.add_cadd import add_cadd_to_vds
from utils.add_dbnsfp import add_dbnsfp_to_vds
from utils.add_clinvar import add_clinvar_to_vds
from utils.add_exac import add_exac_to_vds
from utils.add_gnomad import add_gnomad_to_vds
from utils.add_mpc import add_mpc_to_vds
from utils.computed_fields_utils import CONSEQUENCE_TERMS

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


p = argparse.ArgumentParser()
p.add_argument("--skip-vep", action="store_true", help="Don't run vep.")
p.add_argument("--skip-annotations", action="store_true", help="Don't add any reference data. Intended for testing.")
p.add_argument('--subset', const="X:31097677-33339441", nargs='?',
    help="All data will first be subsetted to this chrom:start-end range. Intended for testing.")
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)
p.add_argument("-i", "--index-name", help="Elasticsearch index name", required=True)
p.add_argument("--split-coding-and-non-coding", action="store_true")

p.add_argument("--exclude-dbnsfp", action="store_true", help="Don't add annotations from dbnsfp. Intended for testing.")
p.add_argument("--exclude-1kg", action="store_true", help="Don't add 1kg scores. Intended for testing.")
p.add_argument("--exclude-cadd", action="store_true", help="Don't add CADD scores (they take a really long time to load). Intended for testing.")
p.add_argument("--exclude-gnomad", action="store_true", help="Don't add gnomAD exome or genome fields. Intended for testing.")
p.add_argument("--exclude-exac", action="store_true", help="Don't add ExAC fields. Intended for testing.")

p.add_argument("--fam-file", help=".fam file used to check VDS sample IDs and assign samples to indices with "
    "a max of 'num_samples' per index, but making sure that samples from the same family don't end up in different indices")
p.add_argument("--max-samples-per-index", help="Max samples per index", type=int, default=MAX_SAMPLES_PER_INDEX)
p.add_argument("--ignore-extra-sample-ids-in-fam-file", action="store_true")
p.add_argument("--ignore-extra-sample-ids-in-vds", action="store_true")
p.add_argument("input_vds", help="input VDS")
p.add_argument("output_vds", nargs="?", help="output vds")

# parse args
args = p.parse_args()

input_vds_path = str(args.input_vds)
if not input_vds_path.endswith(".vds"):
    p.error("Input must be a .vds")

input_vds_path_prefix = input_vds_path.replace(".vds", "")

logger.info("\n==> create HailContext")
hc = hail.HailContext(log="/hail.log")

logger.info("\n==> import vds: " + input_vds_path)
vds = hc.read(input_vds_path)

# compute sample groups
if len(vds.sample_ids) > args.max_samples_per_index:
    if not args.fam_file:
        p.exit("--fam-file must be specified for callsets larger than %s samples. This callset has %s samples." % (args.max_samples_per_index, len(vds.sample_ids)))
    else:
        sample_groups = compute_sample_groups_from_fam_file(
            args.fam_file,
            vds.sample_ids,
            args.ignore_extra_sample_ids_in_vds,
            args.ignore_extra_sample_ids_in_fam_file
        )
else:
    sample_groups = [vds.sample_ids]

filter_interval = "1-MT"
if args.subset:
    filter_interval = args.subset

logger.info("\n==> set filter interval to: %s" % (filter_interval, ))

# split_multi and drop super-contigs and star alleles
vds = vds.filter_intervals(hail.Interval.parse(filter_interval))
vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())
vds = vds.split_multi()
vds = vds.filter_alleles('v.altAlleles[aIndex-1].isStar()', keep=False)
summary = vds.summarize()
pprint(summary)
if summary.variants == 0:
    p.error("0 variants in VDS. Make sure chromosome names don't contain 'chr'")

# run vep
if not args.skip_vep:
    vds = vds.vep(config="/vep/vep-gcloud.properties", root='va.vep', block_size=500)

# add computed annotations
vds_computed_annotation_exprs = [
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

# apply schema to dataset
INPUT_SCHEMA = {
    "top_level_fields": """
            variantId: String,
            originalAltAlleles: Set[String],

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

            vep: Struct,
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
    AC_Het: Array[Int],
    AC_Hom: Array[Int],
    AC_Hemi: Array[Int],
    AN: Int,
    AN_Adj: Int,
    AF: Array[Double],
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
    AF_POPMAX: Float,
"""

GNOMAD_TOP_LEVEL_FIELDS = """filters: Set[String],"""
GNOMAD_INFO_FIELDS = """
    AC: Array[Int],
    Hom: Array[Int],
    Hemi: Array[Int],
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
    AF_POPMAX: Array[Double],
    POPMAX: Array[String],
"""

if not args.skip_annotations:
    logger.info("\n==> add clinvar")
    vds = add_clinvar_to_vds(hc, vds, args.genome_version, root="va.clinvar", info_fields=CLINVAR_INFO_FIELDS, subset=filter_interval)
    #print(vds.summarize())

if not args.skip_annotations and not args.exclude_cadd:
    logger.info("\n==> add cadd")
    vds = add_cadd_to_vds(hc, vds, args.genome_version, root="va.cadd", info_fields=CADD_INFO_FIELDS, subset=filter_interval)
    print(vds.summarize())

if not args.skip_annotations and not args.exclude_dbnsfp:
    logger.info("\n==> add dbnsfp")
    vds = add_dbnsfp_to_vds(hc, vds, args.genome_version, root="va.dbnsfp", subset=filter_interval)

if not args.skip_annotations and not args.exclude_1kg:
    logger.info("\n==> add 1kg")
    vds = add_1kg_phase3_to_vds(hc, vds, args.genome_version, root="va.g1k", subset=filter_interval)

if not args.skip_annotations and not args.exclude_exac:
    logger.info("\n==> add exac")
    vds = add_exac_to_vds(hc, vds, args.genome_version, root="va.exac", top_level_fields=EXAC_TOP_LEVEL_FIELDS, info_fields=EXAC_INFO_FIELDS, subset=filter_interval)
    print(vds.summarize())

if not args.skip_annotations and not args.exclude_gnomad:
    logger.info("\n==> add gnomad exomes")
    vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="exomes", root="va.gnomad_exomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS, subset=filter_interval)
    print(vds.summarize())

if not args.skip_annotations and not args.exclude_gnomad:
    logger.info("\n==> add gnomad genomes")
    vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="genomes", root="va.gnomad_genomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS, subset=filter_interval)
    print(vds.summarize())

logger.info("\n==> adding other annotations")

if args.split_coding_and_non_coding:
    only_coding_or_only_non_coding_settings = [(True, False), (False, True)]
else:
    only_coding_or_only_non_coding_settings = [(False, False)]

for only_coding, only_non_coding in only_coding_or_only_non_coding_settings:
    final_vds = vds

    if not only_non_coding:
        logger.info("\n==> add mpc")
        final_vds = add_mpc_to_vds(hc, final_vds, args.genome_version, root="va.mpc", info_fields=MPC_INFO_FIELDS, subset=filter_interval)

    vds_computed_annotation_exprs = [
        "va.geneIds = %s" % get_expr_for_vep_gene_ids_set(vep_root="va.vep"),
        "va.codingGeneIds = %s" % get_expr_for_vep_gene_ids_set(vep_root="va.vep", only_coding_genes=True),
        "va.transcriptIds = %s" % get_expr_for_vep_transcript_ids_set(vep_root="va.vep"),
        "va.transcriptConsequenceTerms = %s" % get_expr_for_vep_consequence_terms_set(vep_root="va.vep"),
        "va.sortedTranscriptConsequences = %s" % get_expr_for_vep_sorted_transcript_consequences_array(vep_root="va.vep", include_coding_annotations=not only_non_coding),
        "va.mainTranscript = %s" % get_expr_for_worst_transcript_consequence_annotations_struct("va.sortedTranscriptConsequences", include_coding_annotations=not only_non_coding),
        "va.sortedTranscriptConsequences = json(va.sortedTranscriptConsequences)",
    ]

    for expr in vds_computed_annotation_exprs:
        final_vds = final_vds.annotate_variants_expr(expr)

    # now that derived fields have been computed, drop the full vep annotations
    final_vds = final_vds.annotate_variants_expr("va = drop(va, vep)")

    # filter to coding or non-coding variants
    non_coding_consequence_first_index = CONSEQUENCE_TERMS.index("5_prime_UTR_variant")
    if only_coding:
        logger.info("\n==> filter to coding variants only (all transcript consequences above 5_prime_UTR_variant)")
        final_vds = final_vds.filter_variants_expr("va.mainTranscript.major_consequence_rank < %d" % non_coding_consequence_first_index, keep=True)
    elif only_non_coding:
        logger.info("\n==> filter to non-coding variants only (all transcript consequences above 5_prime_UTR_variant)")
        final_vds = final_vds.filter_variants_expr("isMissing(va.mainTranscript.major_consequence_rank) || va.mainTranscript.major_consequence_rank >= %d" % non_coding_consequence_first_index, keep=True)

    if args.output_vds:
        output_vds_path = args.output_vds
    else:
        output_vds_path = input_vds_path_prefix
    
    output_vds_path = output_vds_path.replace(".vds", "")
    if only_coding:
        output_vds_path += ".coding.vds"
    elif only_non_coding:
        output_vds_path += ".non_coding.vds"
    else:
        output_vds_path += ".annotated.vds"

    logger.info("Input: " + input_vds_path)
    logger.info("Output: " + output_vds_path)

    #logger.info("\n==> saving to " + output_vds_path)
    #final_vds.write(output_vds_path, overwrite=True)

    logger.info("Wrote file: " + output_vds_path)
    pprint(final_vds.variant_schema)
    #pprint(final_vds.summarize())

    # host = "10.48.7.5"
    # host="10.56.1.5"
    # rare_genomes_project_v2
    # niaid-gatk3dot4

    #host="10.48.5.5"
    host = "10.4.0.29"
    port = "9200"
    index_name = args.index_name + ("__coding" if only_coding else ("__non_coding" if only_non_coding else ""))
    index_type = "variant"
    block_size = 1000
    num_shards = 4
    DISABLE_INDEX_FOR_FIELDS = ("sortedTranscriptConsequences", )
    DISABLE_DOC_VALUES_FOR_FIELDS = ("sortedTranscriptConsequences", )

    for i, sample_group in enumerate(sample_groups):
        if len(sample_groups) > 1:
            vds_sample_subset = final_vds.filter_samples_list(sample_group, keep=True)
            index_name = "%s_%s" % (args.index_name, i)
        else:
            vds_sample_subset = final_vds
            index_name = args.index_name

        logger.info("==> loading %s samples into %s" % (len(sample_group), index_name))
        logger.info("Samples: %s .. %s" % (", ".join(sample_group[:3]), ", ".join(sample_group[-3:])))


        logger.info("==> export to elasticsearch")
        DISABLE_INDEX_FOR_FIELDS = ("sortedTranscriptConsequences", )
        DISABLE_DOC_VALUES_FOR_FIELDS = ("sortedTranscriptConsequences", )

        timestamp1 = time.time()
        export_vds_to_elasticsearch(
            vds_sample_subset,
            export_genotypes=True,
            host=host,
            port=port,
            index_name=index_name,
            index_type_name=index_type,
            block_size=block_size,
            num_shards=num_shards,
            delete_index_before_exporting=True,
            disable_doc_values_for_fields=DISABLE_DOC_VALUES_FOR_FIELDS,
            disable_index_for_fields=DISABLE_INDEX_FOR_FIELDS,
            is_split_vds=True,
            verbose=True,
        )

        timestamp2 = time.time()
        logger.info("==> finished exporting - time: %s seconds" % (timestamp2 - timestamp1))



# see https://hail.is/hail/annotationdb.html#query-builder
#final_vds = final_vds.annotate_variants_db([
#    'va.cadd.PHRED',
#    'va.cadd.RawScore',
#    'va.dann.score',
#])
