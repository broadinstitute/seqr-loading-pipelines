#!/usr/bin/env python

import os
os.system("pip install elasticsearch")

import argparse
import datetime
import json
import logging
import requests
import time
import sys
from pprint import pprint


from utils.add_gnomad_coverage import add_gnomad_exome_coverage_to_vds, \
    add_gnomad_genome_coverage_to_vds
from utils.add_hgmd import add_hgmd_to_vds
from utils.computed_fields_utils import get_expr_for_variant_id, \
    get_expr_for_vep_gene_ids_set, get_expr_for_vep_transcript_ids_set, \
    get_expr_for_orig_alt_alleles_set, get_expr_for_vep_consequence_terms_set, \
    get_expr_for_vep_sorted_transcript_consequences_array, \
    get_expr_for_worst_transcript_consequence_annotations_struct, get_expr_for_end_pos, \
    get_expr_for_xpos, get_expr_for_contig, get_expr_for_start_pos, get_expr_for_alt_allele, \
    get_expr_for_ref_allele
from utils.elasticsearch_utils import DEFAULT_GENOTYPE_FIELDS_TO_EXPORT, \
    DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP, \
    ELASTICSEARCH_UPSERT, ELASTICSEARCH_INDEX, ELASTICSEARCH_UPDATE
from utils.elasticsearch_client import ElasticsearchClient
from utils.fam_file_utils import MAX_SAMPLES_PER_INDEX, compute_sample_groups_from_fam_file
from utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr
from utils.add_1kg_phase3 import add_1kg_phase3_to_vds
from utils.add_cadd import add_cadd_to_vds
from utils.add_dbnsfp import add_dbnsfp_to_vds
from utils.add_clinvar import add_clinvar_to_vds
from utils.add_exac import add_exac_to_vds
from utils.add_gnomad import add_gnomad_to_vds
from utils.add_topmed import add_topmed_to_vds
from utils.add_mpc import add_mpc_to_vds


logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


p = argparse.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)

p.add_argument("--skip-vep", action="store_true", help="Don't run vep.")
p.add_argument("--add-all-annotations", action="store_true", help="Add any reference data.")
p.add_argument('--subset', const="X:31097677-33339441", nargs='?',
               help="All data will first be subsetted to this chrom:start-end range. Intended for testing.")

p.add_argument("-i", "--index-name", help="elasticsearch index name", required=True)

p.add_argument("-H", "--host", help="Elastisearch IP address", default="10.4.0.29")
p.add_argument("-p", "--port", help="Elastisearch port", default="9200")
p.add_argument("-n", "--num-shards", help="Number of index shards", type=int, default=4)
p.add_argument("-b", "--block-size", help="Block size", type=int, default=1000)

p.add_argument("--export-genotypes", action="store_true", help="Export genotype.")
p.add_argument("--export-only-subset-of-info-fields", action="store_true", help="Add only standard fields from the VCF info field.")
p.add_argument("--add-dbnsfp", action="store_true", help="Add annotations from dbnsfp.")
p.add_argument("--add-1kg", action="store_true", help="Add 1kg AFs.")
p.add_argument("--add-cadd", action="store_true", help="Add CADD scores (they take a really long time to load).")
p.add_argument("--add-gnomad", action="store_true", help="Add gnomAD exome or genome fields.")
p.add_argument("--add-exac", action="store_true", help="Add ExAC fields.")
p.add_argument("--add-topmed", action="store_true", help="Add TopMed AFs.")
p.add_argument("--add-clinvar", action="store_true", help="Add clinvar fields.")
p.add_argument("--add-hgmd", action="store_true", help="Add HGMD fields.")
p.add_argument("--add-mpc", action="store_true", help="Add MPC fields.")
p.add_argument("--add-gnomad-coverage", action="store_true", help="Add gnomAD exome and genome coverage.")
p.add_argument("--start-with-step", help="Which pipeline step to start with.", type=int, default=0)

p.add_argument("--dry-run", action="store_true", help="Don't actually write to elasticsearch")

p.add_argument("input_vds", help="input VDS")

# parse args
args = p.parse_args()

# generate the index name as:  <project>_<WGS_WES>_<family?>_<VARIANTS or SVs>_<YYYYMMDD>_<batch>
index_name = args.index_name.lower()

def read_in_dataset(input_path, filter_interval):
    """Utility method for reading in a .vcf or .vds dataset

    Args:
        input_path (str):
        filter_interval (str):
    """

    logger.info("\n==> Import: " + input_path)
    if input_path.endswith(".vds"):
        vds = hc.read(input_path)
    else:
        vds = hc.import_vcf(input_path, force_bgz=True, min_partitions=10000, generic=True)

    if vds.num_partitions() < 1000:
        vds = vds.repartition(1000, shuffle=True)

    logger.info("\n==> Set filter interval to: %s" % (filter_interval, ))
    vds = vds.filter_intervals(hail.Interval.parse(filter_interval))

    vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())
    if vds.was_split():
        vds = vds.annotate_variants_expr('va.aIndex = 1, va.wasSplit = false')  # isDefined(va.wasSplit)
    else:
        vds = vds.split_multi()

    logger.info("Callset stats:")
    summary = vds.summarize()
    pprint(summary)
    total_variants = summary.variants

    if total_variants == 0:
        raise ValueError("0 variants in VDS. Make sure chromosome names don't contain 'chr'")
    else:
        logger.info("\n==> Total variants: %s" % (total_variants,))

    return vds




# ==========================
# reference dataset schemas
# ==========================

# add reference data
CLINVAR_INFO_FIELDS = """
    --- variation_type: String,
    variation_id: String,
    --- rcv: String,
    --- scv: String,
    allele_id: Int,
    clinical_significance: String,
    pathogenic: Int,
    likely_pathogenic: Int,
    uncertain_significance: Int,
    likely_benign: Int,
    benign: Int,
    conflicted: Int,
    --- gold_stars: String,
    review_status: String,
    all_submitters: String,
    --- all_traits: String,
    --- all_pmids: String,
    inheritance_modes: String,
    --- age_of_onset: String,
    --- prevalence: String,
    --- disease_mechanism: String,
    --- origin: String,
    --- xrefs: String,
"""

CADD_INFO_FIELDS = """
    PHRED: Double,
    --- RawScore: Double,
"""

MPC_INFO_FIELDS = """
    MPC: Double,
    --- fitted_score: Double,
    --- mis_badness: Double,
    --- obs_exp: Double,
"""

EXAC_TOP_LEVEL_FIELDS = """filters: Set[String],"""
EXAC_INFO_FIELDS = """
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
    POPMAX: Array[String],
"""

GNOMAD_TOP_LEVEL_FIELDS = """filters: Set[String],"""
GNOMAD_INFO_FIELDS = """
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
    AF_POPMAX: Array[Double],
    POPMAX: Array[String],
"""



input_path = str(args.input_vds)
if not (input_path.endswith(".vds") or input_path.endswith(".vcf") or input_path.endswith(".vcf.gz") or input_path.endswith(".vcf.bgz")):
    p.error("Input must be a .vds or .vcf.gz")

input_path_prefix = input_path.replace(".vds", "")

elasticsearch_url = "http://%s:%s" % (args.host, args.port)
response = requests.get(elasticsearch_url)
elasticsearch_response = json.loads(response.content)
if "tagline" not in elasticsearch_response:
    p.error("Unexpected response from %s: %s" % (elasticsearch_url, elasticsearch_response))
else:
    logger.info("Connected to %s: %s" % (elasticsearch_url, elasticsearch_response["tagline"]))

filter_interval = "1-MT"
if args.subset:
    filter_interval = args.subset


logger.info("\n==> Create HailContext")

import hail  # import hail here so that you can run this script with --help even if hail isn't installed locally.
hc = hail.HailContext(log="/hail.log")

logger.info("Reading in dataset...")

output_vds_prefix = input_path.replace(".vcf", "").replace(".vds", "").replace(".bgz", "").replace(".gz", "").replace(".vep", "")

vep_output_vds = output_vds_prefix + ".vep.vds"
annotated_output_vds = output_vds_prefix + ".vep_and_annotations.vds"

# run vep
if args.start_with_step == 0 and not args.skip_vep:
    logger.info("=============================== pipeline - step 0 ===============================")
    logger.info("Read in data, run vep")

    vds = read_in_dataset(input_path, filter_interval)
    vds = vds.vep(config="/vep/vep-gcloud.properties", root='va.vep', block_size=500)

    logger.info("Writing vds to: " + vep_output_vds)
    vds.write(vep_output_vds, overwrite=True)

    hc.stop()

    logger.info("\n==> Re-create HailContext")
    hc = hail.HailContext(log="/hail.log")

logger.info("=============================== pipeline - step 1 ===============================")
logger.info("Read in data, compute various derived fields, export to elasticsearch")


vds = read_in_dataset(vep_output_vds, filter_interval)

# add computed annotations
logger.info("\n==> Adding computed annotations")
parallel_computed_annotation_exprs = [
    "va.docId = %s" % get_expr_for_variant_id(512),
    "va.variantId = %s" % get_expr_for_variant_id(),

    "va.contig = %s" % get_expr_for_contig(),
    "va.start = %s" % get_expr_for_start_pos(),
    "va.pos = %s" % get_expr_for_start_pos(),
    "va.end = %s" % get_expr_for_end_pos(),
    "va.ref = %s" % get_expr_for_ref_allele(),
    "va.alt = %s" % get_expr_for_alt_allele(),

    "va.xpos = %s" % get_expr_for_xpos(pos_field="start"),
    "va.xstart = %s" % get_expr_for_xpos(pos_field="start"),

    "va.geneIds = %s" % get_expr_for_vep_gene_ids_set(vep_root="va.vep"),
    "va.codingGeneIds = %s" % get_expr_for_vep_gene_ids_set(vep_root="va.vep", only_coding_genes=True),
    "va.transcriptIds = %s" % get_expr_for_vep_transcript_ids_set(vep_root="va.vep"),
    "va.transcriptConsequenceTerms = %s" % get_expr_for_vep_consequence_terms_set(vep_root="va.vep"),
    "va.sortedTranscriptConsequences = %s" % get_expr_for_vep_sorted_transcript_consequences_array(vep_root="va.vep"),
]

serial_computed_annotation_exprs = [
    "va.xstop = %s" % get_expr_for_xpos(field_prefix="va.", pos_field="end"),
    "va.mainTranscript = %s" % get_expr_for_worst_transcript_consequence_annotations_struct("va.sortedTranscriptConsequences"),
    "va.sortedTranscriptConsequences = json(va.sortedTranscriptConsequences)",
]

vds = vds.annotate_variants_expr(parallel_computed_annotation_exprs)

for expr in serial_computed_annotation_exprs:
    vds = vds.annotate_variants_expr(expr)

pprint(vds.variant_schema)

# apply schema to dataset
INPUT_SCHEMA  = {}
INPUT_SCHEMA["top_level_fields"] = """
    docId: String,
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

    --- rsid: String,
    --- qual: Double,
    --- filters: Set[String],
    wasSplit: Boolean,
    aIndex: Int,

    geneIds: Set[String],
    transcriptIds: Set[String],
    codingGeneIds: Set[String],
    transcriptConsequenceTerms: Set[String],
    sortedTranscriptConsequences: String,
    mainTranscript: Struct,
"""

INPUT_SCHEMA["info_fields"] = """
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
    --- VQSLOD: Double,
    --- culprit: String,
"""

if args.export_only_subset_of_info_fields:
    expr = convert_vds_schema_string_to_annotate_variants_expr(root="va.clean", **INPUT_SCHEMA)
    vds = vds.annotate_variants_expr(expr=expr)
    vds = vds.annotate_variants_expr("va = va.clean")
else:
    vds = vds.annotate_variants_expr(expr="va.clean = va.info")

    INPUT_SCHEMA["info_fields"] = ""
    expr = convert_vds_schema_string_to_annotate_variants_expr(root="va.clean", **INPUT_SCHEMA)
    vds = vds.annotate_variants_expr(expr=expr)
    vds = vds.annotate_variants_expr("va = va.clean")

if args.export_genotypes:
    genotype_fields_to_export = DEFAULT_GENOTYPE_FIELDS_TO_EXPORT
    genotype_field_to_elasticsearch_type_map = DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP
else:
    genotype_fields_to_export = []
    genotype_field_to_elasticsearch_type_map = {}

vds = vds.persist()

if args.add_all_annotations or args.add_clinvar:
    logger.info("\n==> Add clinvar")
    vds = add_clinvar_to_vds(hc, vds, args.genome_version, root="va.clinvar", subset=filter_interval)

if args.add_all_annotations or args.add_hgmd:
    logger.info("\n==> Add hgmd")
    vds = add_hgmd_to_vds(hc, vds, args.genome_version, root="va.hgmd", subset=filter_interval)


if args.add_all_annotations or args.add_cadd:
    logger.info("\n==> Add cadd")
    vds = add_cadd_to_vds(hc, vds, args.genome_version, root="va.cadd", info_fields=CADD_INFO_FIELDS, subset=filter_interval)

if args.add_all_annotations or args.add_dbnsfp:
    logger.info("\n==> Add dbnsfp")
    vds = add_dbnsfp_to_vds(hc, vds, args.genome_version, root="va.dbnsfp", subset=filter_interval)

if args.add_all_annotations or args.add_1kg:
    logger.info("\n==> Add 1kg")
    vds = add_1kg_phase3_to_vds(hc, vds, args.genome_version, root="va.g1k", subset=filter_interval)

if args.add_all_annotations or args.add_exac:
    logger.info("\n==> Add exac")
    vds = add_exac_to_vds(hc, vds, args.genome_version, root="va.exac", top_level_fields=EXAC_TOP_LEVEL_FIELDS, info_fields=EXAC_INFO_FIELDS, subset=filter_interval)

if args.add_all_annotations or args.add_topmed and args.genome_version == "38":
    logger.info("\n==> Add topmed")
    vds = add_topmed_to_vds(hc, vds, args.genome_version, root="va.topmed", subset=filter_interval)

if args.add_all_annotations or args.add_mpc:
    logger.info("\n==> Add mpc")
    vds = add_mpc_to_vds(hc, vds, args.genome_version, root="va.mpc", info_fields=MPC_INFO_FIELDS, subset=filter_interval)

if args.add_all_annotations or args.add_gnomad:
    logger.info("\n==> Add gnomad exomes")
    vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="exomes", root="va.gnomad_exomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS, subset=filter_interval)

if args.add_all_annotations or args.add_gnomad:
    logger.info("\n==> Add gnomad genomes")
    vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="genomes", root="va.gnomad_genomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS, subset=filter_interval)

if args.add_all_annotations or args.add_gnomad_coverage:
    logger.info("\n==> Add gnomad coverage")
    vds = vds.persist()
    vds = add_gnomad_exome_coverage_to_vds(hc, vds, args.genome_version, root="va.gnomad_exome_coverage")
    vds = add_gnomad_genome_coverage_to_vds(hc, vds, args.genome_version, root="va.gnomad_genome_coverage")

vds = vds.persist()

# write to VDS
client = ElasticsearchClient(args.host, args.port)
timestamp1 = time.time()

if args.dry_run:
    logger.info("Dry run finished. Next step would be to export to index: " + str(index_name))
else:
    client.export_vds_to_elasticsearch(
        vds,
        genotype_fields_to_export=genotype_fields_to_export,
        genotype_field_to_elasticsearch_type_map=genotype_field_to_elasticsearch_type_map,
        index_name=index_name,
        index_type_name="variant",
        block_size=args.block_size,
        num_shards=args.num_shards,
        delete_index_before_exporting=True,
        elasticsearch_write_operation=ELASTICSEARCH_INDEX,
        elasticsearch_mapping_id="docId",
        disable_doc_values_for_fields=("sortedTranscriptConsequences", ),
        disable_index_for_fields=("sortedTranscriptConsequences", ),
        is_split_vds=True,
        verbose=True,
    )

    timestamp2 = time.time()
    logger.info("==> Finished exporting - time: %s seconds" % (timestamp2 - timestamp1))

hc.stop()


