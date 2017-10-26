#!/usr/bin/env python
import argparse
import hail
import json
import logging
import requests
import sys
import time
from pprint import pprint

from utils.computed_fields_utils import get_expr_for_variant_id, \
    get_expr_for_vep_gene_ids_set, get_expr_for_vep_transcript_ids_set, \
    get_expr_for_orig_alt_alleles_set, get_expr_for_vep_consequence_terms_set, \
    get_expr_for_vep_sorted_transcript_consequences_array, \
    get_expr_for_worst_transcript_consequence_annotations_struct, get_expr_for_end_pos, \
    get_expr_for_xpos, get_expr_for_contig, get_expr_for_start_pos, get_expr_for_alt_allele, \
    get_expr_for_ref_allele
from utils.elasticsearch_utils import export_vds_to_elasticsearch, DEFAULT_GENOTYPE_FIELDS_TO_EXPORT, \
    ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE, DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP, \
    ELASTICSEARCH_UPSERT, ELASTICSEARCH_UPDATE
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
p.add_argument('--subset', const="X:31097677-33339441", nargs='?',
    help="All data will first be subsetted to this chrom:start-end range. Intended for testing.")

p.add_argument("-i", "--index-name", help="Elasticsearch index name", required=True)
p.add_argument("-H", "--host", help="Elastisearch IP address", default="10.4.0.29")  # "10.128.0.3"
p.add_argument("-p", "--port", help="Elastisearch port", default="9200")  # "10.128.0.3"
p.add_argument("-n", "--num-shards", help="Number of index shards", type=int, default=4)  # "10.128.0.3"
p.add_argument("-b", "--block-size", help="Block size", type=int, default=1000)  # "10.128.0.3"

p.add_argument("--fam-file", help=".fam file used to check VDS sample IDs and assign samples to indices with "
    "a max of 'num_samples' per index, but making sure that samples from the same family don't end up in different indices")
p.add_argument("--max-samples-per-index", help="Max samples per index", type=int, default=MAX_SAMPLES_PER_INDEX)
p.add_argument("--ignore-extra-sample-ids-in-fam-file", action="store_true")
p.add_argument("--ignore-extra-sample-ids-in-vds", action="store_true")
p.add_argument("-t", "--datatype", help="What pipeline generated the data", choices=["GATK_VARIANTS", "MANTA_SVS"], default="GATK_VARIANTS")
p.add_argument("input_vds", help="input VDS")
p.add_argument("output_vds", nargs="?", help="output vds")

# parse args
args = p.parse_args()

if args.index_name.lower() != args.index_name:
    p.error("Index name must be lowercase")

input_vds_path = str(args.input_vds)
if not (input_vds_path.endswith(".vds") or input_vds_path.endswith(".vcf.gz") or input_vds_path.endswith(".vcf.bgz")):
    p.error("Input must be a .vds or .vcf.gz")

input_vds_path_prefix = input_vds_path.replace(".vds", "")

elasticsearch_url = "http://%s:%s" % (args.host, args.port)
response = requests.get(elasticsearch_url)
elasticsearch_response = json.loads(response.content)
if "tagline" not in elasticsearch_response:
    p.error("Unexpected response from %s: %s" % (elasticsearch_url, elasticsearch_response))
else:
    logger.info("Connected to %s: %s" % (elasticsearch_url, elasticsearch_response["tagline"]))

host = args.host
port = args.port
index_name = args.index_name
block_size = args.block_size
num_shards = args.num_shards
index_type = "variant"

logger.info("\n==> create HailContext")
hc = hail.HailContext(log="/hail.log")

logger.info("\n==> import: " + input_vds_path)
if input_vds_path.endswith(".vds"):
    vds = hc.read(input_vds_path)
else:
    if args.datatype == "GATK_VARIANTS":
        vds = hc.import_vcf(input_vds_path, force_bgz=True, min_partitions=10000)
    elif args.datatype == "MANTA_SVS":
        vds = hc.import_vcf(input_vds_path, force_bgz=True, min_partitions=10000, generic=True)
    else:
        raise ValueError("Unexpected datatype: %s" % args.datatype)

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

if vds.num_partitions() < 1000:
    vds = vds.repartition(1000, shuffle=True)

filter_interval = "1-MT"
if args.subset:
    filter_interval = args.subset
#vds = vds.filter_alleles('v.altAlleles[aIndex-1].isStar()', keep=False)

logger.info("\n==> set filter interval to: %s" % (filter_interval, ))
vds = vds.filter_intervals(hail.Interval.parse(filter_interval))

if args.datatype == "GATK_VARIANTS":
    vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())
    vds = vds.split_multi()
    summary = vds.summarize()
    pprint(summary)
    total_variants = summary.variants
elif args.datatype == "MANTA_SVS":
    vds = vds.annotate_variants_expr('va.aIndex = 1, va.wasSplit = true')
    _, total_variants = vds.count()
else:
    raise ValueError("Unexpected datatype: %s" % args.datatype)

if total_variants == 0:
    p.error("0 variants in VDS. Make sure chromosome names don't contain 'chr'")
else:
    logger.info("\n==> total variants: %s" % (total_variants,))

# add computed annotations
parallel_computed_annotation_exprs = [
    "va.docId = %s" % get_expr_for_variant_id(512),
]

vds = vds.annotate_variants_expr(parallel_computed_annotation_exprs)

pprint(vds.variant_schema)

# apply schema to dataset
INPUT_SCHEMA  = {}
if args.datatype == "GATK_VARIANTS":
    INPUT_SCHEMA["top_level_fields"] = """
        docId: String,
        wasSplit: Boolean,
        aIndex: Int,
    """

    INPUT_SCHEMA["info_fields"] = ""

elif args.datatype == "MANTA_SVS":
    INPUT_SCHEMA["top_level_fields"] = """
        docId: String,
    """

    INPUT_SCHEMA["info_fields"] = ""

else:
    raise ValueError("Unexpected datatype: %s" % args.datatype)

expr = convert_vds_schema_string_to_annotate_variants_expr(root="va.clean", **INPUT_SCHEMA)

vds = vds.annotate_variants_expr(expr=expr)
vds = vds.annotate_variants_expr("va = va.clean")


# add reference data
CLINVAR_INFO_FIELDS = """
    --- MEASURESET_TYPE: String,
    MEASURESET_ID: String,
    --- RCV: String,
    ALLELE_ID: String,
    CLINICAL_SIGNIFICANCE: String,
    --- PATHOGENIC: String,
    --- BENIGN: String,
    --- CONFLICTED: String,
    --- REVIEW_STATUS: String,
    GOLD_STARS: String,
    ALL_SUBMITTERS: String,
    --- ALL_TRAITS: String,
    --- ALL_PMIDS: String,
    INHERITANCE_MODES: String,
    --- AGE_OF_ONSET: String,
    --- PREVALENCE: String,
    --- DISEASE_MECHANISM: String,
    --- ORIGIN: String,
    --- XREFS: String
"""

CADD_INFO_FIELDS = """
    PHRED: Double,
    RawScore: Double,
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
    --- AC_Adj: Array[Int],
    --- AC_Het: Array[Int],
    AC_Hom: Array[Int],
    AC_Hemi: Array[Int],
    --- AN: Int,
    --- AN_Adj: Int,
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
    --- AN: Int,
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

logger.info("\n==> add cadd")
vds = add_cadd_to_vds(hc, vds, args.genome_version, root="va.cadd", info_fields=CADD_INFO_FIELDS, subset=filter_interval)

logger.info("\n==> add gnomad exomes")
vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="exomes", root="va.gnomad_exomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS, subset=filter_interval)

logger.info("\n==> add gnomad genomes")
vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="genomes", root="va.gnomad_genomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS, subset=filter_interval)

#logger.info("\n==> add dbnsfp")
#vds = add_dbnsfp_to_vds(hc, vds, args.genome_version, root="va.dbnsfp", subset=filter_interval)

logger.info("\n==> add 1kg")
vds = add_1kg_phase3_to_vds(hc, vds, args.genome_version, root="va.g1k", subset=filter_interval)

logger.info("\n==> add exac")
vds = add_exac_to_vds(hc, vds, args.genome_version, root="va.exac", top_level_fields=EXAC_TOP_LEVEL_FIELDS, info_fields=EXAC_INFO_FIELDS)

logger.info("\n==> add topmed")
vds = add_topmed_to_vds(hc, vds, args.genome_version, root="va.topmed", subset=filter_interval)

#logger.info("\n==> add clinvar")
#vds = add_clinvar_to_vds(hc, vds, args.genome_version, root="va.clinvar", info_fields=CLINVAR_INFO_FIELDS, subset=filter_interval)

logger.info("\n==> add mpc")
vds = add_mpc_to_vds(hc, vds, args.genome_version, root="va.mpc", info_fields=MPC_INFO_FIELDS, subset=filter_interval)


# filter to coding or non-coding variants
if args.output_vds:
    output_vds_path = args.output_vds
else:
    output_vds_path = input_vds_path_prefix

#output_vds_path = output_vds_path.replace(".vds", "") + ".annotated.vds"

logger.info("Input: " + input_vds_path)
#logger.info("Output: " + output_vds_path)
#logger.info("\n==> saving to " + output_vds_path)
#vds.write(output_vds_path, overwrite=True)
#logger.info("Wrote file: " + output_vds_path)

# host = "10.48.7.5"
# host="10.56.1.5"
# rare_genomes_project_v2
# niaid-gatk3dot4


if args.datatype == "GATK_VARIANTS":
    genotype_fields_to_export = DEFAULT_GENOTYPE_FIELDS_TO_EXPORT
    genotype_field_to_elasticsearch_type_map = DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP
elif args.datatype == "MANTA_SVS":
    genotype_fields_to_export = [
        'num_alt = if(g.GT.isCalled()) g.GT.nNonRefAlleles() else -1',
        'genotype_filter = g.FT',
        'gq = g.GQ',
        'dp = if(g.GT.isCalled()) [g.PR.sum + g.SR.sum, '+ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE+'].min() else NA:Int',
        'ab = let total=g.PR.sum + g.SR.sum in if(g.GT.isCalled() && total != 0) ((g.PR[1] + g.SR[1]) / total).toFloat else NA:Float',
        'ab_PR = let total=g.PR.sum in if(g.GT.isCalled() && total != 0) (g.PR[1] / total).toFloat else NA:Float',
        'ab_SR = let total=g.SR.sum in if(g.GT.isCalled() && total != 0) (g.SR[1] / total).toFloat else NA:Float',
        'dp_PR = if(g.GT.isCalled()) [g.PR.sum,'+ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE+'].min() else NA:Int',
        'dp_SR = if(g.GT.isCalled()) [g.SR.sum,'+ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE+'].min() else NA:Int',
    ]
    genotype_field_to_elasticsearch_type_map = {
        ".*_num_alt": {"type": "byte", "doc_values": "false"},
        ".*_genotype_filter": {"type": "keyword", "doc_values": "false"},
        ".*_gq": {"type": "short", "doc_values": "false"},
        ".*_dp": {"type": "short", "doc_values": "false"},
        ".*_ab": {"type": "half_float", "doc_values": "false"},
        ".*_ab_PR": {"type": "half_float", "doc_values": "false"},
        ".*_ab_SR": {"type": "half_float", "doc_values": "false"},
        ".*_dp_PR": {"type": "short", "doc_values": "false"},
        ".*_dp_SR": {"type": "short", "doc_values": "false"},
    }
else:
    raise ValueError("Unexpected datatype: %s" % args.datatype)


for i, sample_group in enumerate(sample_groups):
    if len(sample_groups) > 1:
        vds_sample_subset = vds.filter_samples_list(sample_group, keep=True)
        index_name = "%s_%s" % (args.index_name, i)
    else:
        vds_sample_subset = vds
        index_name = args.index_name

    logger.info("==> loading %s samples into %s" % (len(sample_group), index_name))
    logger.info("Samples: %s .. %s" % (", ".join(sample_group[:3]), ", ".join(sample_group[-3:])))

    logger.info("==> export to elasticsearch")

    if i == 0:
        print("skipping 0...")

    timestamp1 = time.time()
    export_vds_to_elasticsearch(
        vds_sample_subset,
        genotype_fields_to_export=[],
        genotype_field_to_elasticsearch_type_map={},
        host=host,
        port=port,
        index_name=index_name,
        index_type_name=index_type,
        block_size=block_size,
        num_shards=num_shards,
        delete_index_before_exporting=False,
        elasticsearch_write_operation=ELASTICSEARCH_UPDATE,
        elasticsearch_mapping_id="docId",
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
