#!/usr/bin/env python
import argparse
import hail
import json
import logging
import requests
import time
from pprint import pprint, pformat

from utils.add_gnomad_coverage import add_gnomad_exome_coverage_to_vds, \
    add_gnomad_genome_coverage_to_vds
from utils.computed_fields_utils import get_expr_for_variant_id, \
    get_expr_for_vep_gene_ids_set, get_expr_for_vep_transcript_ids_set, \
    get_expr_for_orig_alt_alleles_set, get_expr_for_vep_consequence_terms_set, \
    get_expr_for_vep_sorted_transcript_consequences_array, \
    get_expr_for_worst_transcript_consequence_annotations_struct, get_expr_for_end_pos, \
    get_expr_for_xpos, get_expr_for_contig, get_expr_for_start_pos, get_expr_for_alt_allele, \
    get_expr_for_ref_allele
from utils.elasticsearch_utils import DEFAULT_GENOTYPE_FIELDS_TO_EXPORT, \
    ELASTICSEARCH_MAX_SIGNED_SHORT_INT_TYPE, DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP, \
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


def read_in_dataset(input_path, datatype, filter_interval):
    """Utility method for reading in a .vcf or .vds dataset

    Args:
        input_path (str):
        datatype (str):
        filter_interval (str):
    """

    logger.info("\n==> import: " + input_path)
    if input_path.endswith(".vds"):
        vds = hc.read(input_path)
    else:
        if datatype == "GATK_VARIANTS":
            vds = hc.import_vcf(input_path, force_bgz=True, min_partitions=10000)
        elif datatype == "MANTA_SVS":
            vds = hc.import_vcf(input_path, force_bgz=True, min_partitions=10000, generic=True)
        else:
            raise ValueError("Unexpected datatype: %s" % datatype)

    if vds.num_partitions() < 1000:
        vds = vds.repartition(1000, shuffle=True)

    #vds = vds.filter_alleles('v.altAlleles[aIndex-1].isStar()', keep=False)

    logger.info("\n==> set filter interval to: %s" % (filter_interval, ))
    vds = vds.filter_intervals(hail.Interval.parse(filter_interval))

    if datatype == "GATK_VARIANTS":
        vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())
        if vds.was_split():
            vds = vds.annotate_variants_expr('va.aIndex = 1, va.wasSplit = false')  # isDefined(va.wasSplit)
        else:
            vds = vds.split_multi()

        summary = vds.summarize()
        pprint(summary)
        total_variants = summary.variants
    elif datatype == "MANTA_SVS":
        vds = vds.annotate_variants_expr('va.aIndex = 1, va.wasSplit = false')
        _, total_variants = vds.count()
    else:
        raise ValueError("Unexpected datatype: %s" % datatype)

    if total_variants == 0:
        raise ValueError("0 variants in VDS. Make sure chromosome names don't contain 'chr'")
    else:
        logger.info("\n==> total variants: %s" % (total_variants,))

    return vds


def compute_minimal_schema(vds, datatype):

    # add computed annotations
    parallel_computed_annotation_exprs = [
        "va.docId = %s" % get_expr_for_variant_id(512),
    ]

    vds = vds.annotate_variants_expr(parallel_computed_annotation_exprs)

    #pprint(vds.variant_schema)

    # apply schema to dataset
    INPUT_SCHEMA  = {}
    if datatype == "GATK_VARIANTS":
        INPUT_SCHEMA["top_level_fields"] = """
            docId: String,
            wasSplit: Boolean,
            aIndex: Int,
        """

        INPUT_SCHEMA["info_fields"] = ""

    elif datatype == "MANTA_SVS":
        INPUT_SCHEMA["top_level_fields"] = """
            docId: String,
        """

        INPUT_SCHEMA["info_fields"] = ""

    else:
        raise ValueError("Unexpected datatype: %s" % datatype)

    expr = convert_vds_schema_string_to_annotate_variants_expr(root="va.clean", **INPUT_SCHEMA)
    vds = vds.annotate_variants_expr(expr=expr)
    vds = vds.annotate_variants_expr("va = va.clean")

    return vds


def export_to_elasticsearch(
    host,
    port,
    vds,
    index_name,
    datatype,
    block_size,
    num_shards,
    operation=ELASTICSEARCH_INDEX,
    delete_index_before_exporting=False,
    export_genotypes=True,
    disable_doc_values_for_fields=(),
    disable_index_for_fields=(),
    export_snapshot_to_google_bucket=False,
    start_with_sample_group = 0,
):
    """Utility method for exporting the given vds to an elasticsearch index.
    """

    logger.info("Input: " + input_path)

    index_type = "variant"

    if export_genotypes:
        if datatype == "GATK_VARIANTS":
            genotype_fields_to_export = DEFAULT_GENOTYPE_FIELDS_TO_EXPORT
            genotype_field_to_elasticsearch_type_map = DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP
        elif datatype == "MANTA_SVS":
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
            raise ValueError("Unexpected datatype: %s" % datatype)
    else:
        genotype_fields_to_export = []
        genotype_field_to_elasticsearch_type_map = {}

    vds = vds.persist()

    client = ElasticsearchClient(host, port)
    for i, sample_group in enumerate(sample_groups):

        if i < start_with_sample_group:
            continue

        #if delete_index_before_exporting and i < 4:
        #    continue

        if len(sample_groups) > 1:
            vds_sample_subset = vds.filter_samples_list(sample_group, keep=True)
            current_index_name = "%s_%s" % (index_name, i)
        else:
            vds_sample_subset = vds
            current_index_name = index_name

        logger.info("==> exporting %s samples into %s" % (len(sample_group), current_index_name))
        logger.info("Samples: %s .. %s" % (", ".join(sample_group[:3]), ", ".join(sample_group[-3:])))

        logger.info("==> export to elasticsearch")
        timestamp1 = time.time()

        client.export_vds_to_elasticsearch(
            vds_sample_subset,
            genotype_fields_to_export=genotype_fields_to_export,
            genotype_field_to_elasticsearch_type_map=genotype_field_to_elasticsearch_type_map,
            index_name=current_index_name,
            index_type_name=index_type,
            block_size=block_size,
            num_shards=num_shards,
            delete_index_before_exporting=delete_index_before_exporting,
            elasticsearch_write_operation=operation,
            elasticsearch_mapping_id="docId",
            disable_doc_values_for_fields=disable_doc_values_for_fields,
            disable_index_for_fields=disable_index_for_fields,
            is_split_vds=True,
            verbose=True,
        )

        timestamp2 = time.time()
        logger.info("==> finished exporting - time: %s seconds" % (timestamp2 - timestamp1))

    if export_snapshot_to_google_bucket:

        logger.info("==> export snapshot to google bucket")
        client.create_elasticsearch_snapshot(
            index_name=index_name + "*",
            bucket="seqr-database-backups",
            base_path="elasticsearch/snapshots",
            snapshot_repo="callsets")

#if args.output_vds:
    #    output_vds_path = args.output_vds
    #else:
    #    output_vds_path = input_path_prefix
    #logger.info("Output: " + output_vds_path)
    #logger.info("\n==> saving to " + output_vds_path)
    #vds.write(output_vds_path, overwrite=True)
    #logger.info("Wrote file: " + output_vds_path)



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


p = argparse.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)

p.add_argument("--skip-vep", action="store_true", help="Don't run vep.")
p.add_argument("--skip-annotations", action="store_true", help="Don't add any reference data. Intended for testing.")
p.add_argument('--subset', const="X:31097677-33339441", nargs='?',
    help="All data will first be subsetted to this chrom:start-end range. Intended for testing.")
p.add_argument('--remap-sample-ids', help="Filepath containing 2 tab-separated columns: current sample id and desired sample id")
p.add_argument('--subset-samples', help="Filepath containing ids for samples to keep; if used with --remap_sample_ids, ids are the desired ids (post remapping)")

p.add_argument("-i", "--index-name", help="Elasticsearch index name", required=True)
p.add_argument("-H", "--host", help="Elastisearch IP address", default="10.4.0.29")  # "10.128.0.3"
p.add_argument("-p", "--port", help="Elastisearch port", default="9200")  # "10.128.0.3"
p.add_argument("-n", "--num-shards", help="Number of index shards", type=int, default=4)  # "10.128.0.3"
p.add_argument("-b", "--block-size", help="Block size", type=int, default=1000)  # "10.128.0.3"

p.add_argument("--exclude-dbnsfp", action="store_true", help="Don't add annotations from dbnsfp. Intended for testing.")
p.add_argument("--exclude-1kg", action="store_true", help="Don't add 1kg AFs. Intended for testing.")
p.add_argument("--exclude-cadd", action="store_true", help="Don't add CADD scores (they take a really long time to load). Intended for testing.")
p.add_argument("--exclude-gnomad", action="store_true", help="Don't add gnomAD exome or genome fields. Intended for testing.")
p.add_argument("--exclude-exac", action="store_true", help="Don't add ExAC fields. Intended for testing.")
p.add_argument("--exclude-topmed", action="store_true", help="Don't add TopMed AFs. Intended for testing.")
p.add_argument("--exclude-clinvar", action="store_true", help="Don't add clinvar fields. Intended for testing.")
p.add_argument("--exclude-mpc", action="store_true", help="Don't add MPC fields. Intended for testing.")
p.add_argument("--exclude-gnomad-coverage", action="store_true", help="Don't add gnomAD exome and genome coverage. Intended for testing.")

p.add_argument("--fam-file", help=".fam file used to check VDS sample IDs and assign samples to indices with "
    "a max of 'num_samples' per index, but making sure that samples from the same family don't end up in different indices. "
    "If used with --remap-sample-ids, contains IDs of samples after remapping")
p.add_argument("--max-samples-per-index", help="Max samples per index", type=int, default=MAX_SAMPLES_PER_INDEX)
p.add_argument("--ignore-extra-sample-ids-in-tables", action="store_true")
p.add_argument("--ignore-extra-sample-ids-in-vds", action="store_true")
p.add_argument("--start-with-step", help="Which pipeline step to start with.", type=int, default=0)
p.add_argument("--start-with-sample-group", help="If the callset contains more samples than the limit specified by --max-samples-per-index, "
    "it will be loaded into multiple separate indices. Setting this command-line arg to a value > 0 causes the pipeline to start from sample "
    "group other than the 1st one. This is useful for restarting a failed pipeline from exactly where it left off.", type=int, default=0)
p.add_argument("-t", "--datatype", help="What pipeline generated the data", choices=["GATK_VARIANTS", "MANTA_SVS"], default="GATK_VARIANTS")
p.add_argument("input_vds", help="input VDS")
p.add_argument("output_vds", nargs="?", help="output vds")


# parse args
args = p.parse_args()

if args.index_name.lower() != args.index_name:
    p.error("Index name must be lowercase")

input_path = str(args.input_vds)
if not (input_path.endswith(".vds") or input_path.endswith(".vcf.gz") or input_path.endswith(".vcf.bgz")):
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


logger.info("\n==> create HailContext")
hc = hail.HailContext(log="/hail.log")

logger.info("Reading in dataset...")
vds = read_in_dataset(input_path, args.datatype, filter_interval)

#NOTE: if sample IDs are remapped first thing, then the fam file should contain the desired (not original IDs)
if args.remap_sample_ids:
    logger.info("Remapping sample ids...")
    id_map = hc.import_table(args.remap_sample_ids, impute=True, no_header=True)
    mapping = dict(zip(id_map.query('f0.collect()'), id_map.query('f1.collect()')))
    # check that ids being remapped exist in VDS
    sample_query = set(mapping.keys())
    samples_in_vds = set(vds.sample_ids)
    matched = sample_query.intersection(samples_in_vds)
    if len(matched) < len(sample_query) and args.ignore_extra_sample_ids_in_tables:
        logger.warning('Found only {0} out of {1} samples specified for ID remapping'.format(len(matched), len(sample_query)))
    elif len(matched) < len(sample_query):
        raise ValueError('Found only {0} out of {1} samples specified for ID remapping'.format(len(matched), len(sample_query)))
    vds = vds.rename_samples(mapping)
    logger.info('Remapped {} sample ids...'.format(len(matched)))


# subset samples as desired
if args.subset_samples:
    logger.info("Subsetting to specified samples...")
    keep_samples = hc.import_table(args.subset_samples, impute=True, no_header=True).key_by('f0')
    # check that all subset samples exist in VDS
    sample_query = set(keep_samples.query('f0.collect()'))
    samples_in_vds = set(vds.sample_ids)
    matched = sample_query.intersection(samples_in_vds)
    if len(matched) < len(sample_query) and args.ignore_extra_sample_ids_in_tables:
        logger.warning('Found only {0} out of {1} samples specified for ID remapping'.format(len(matched), len(sample_query)))
    elif len(matched) < len(sample_query):
        raise ValueError('Found only {0} out of {1} samples specified for ID remapping'.format(len(matched), len(sample_query)))
    original_sample_count = vds.num_samples
    vds = vds.filter_samples_table(keep_samples, keep=True)
    new_sample_count = vds.num_samples
    logger.info('Kept {0} out of {1} samples in vds'.format(new_sample_count, original_sample_count))


# compute sample groups
if len(vds.sample_ids) > args.max_samples_per_index:
    if not args.fam_file:
        p.exit("--fam-file must be specified for callsets larger than %s samples. This callset has %s samples." % (args.max_samples_per_index, len(vds.sample_ids)))
    else:
        sample_groups = compute_sample_groups_from_fam_file(
            args.fam_file,
            vds.sample_ids,
            args.max_samples_per_index,
            args.ignore_extra_sample_ids_in_vds,
            args.ignore_extra_sample_ids_in_tables,
        )
else:
    sample_groups = [vds.sample_ids]



# run vep
if not args.skip_vep:
    logger.info("Annotating with VEP...")
    output_vds_prefix = input_path.replace(".vcf", "").replace(".vds", "").replace(".bgz", "").replace(".gz", "").replace(".vep", "")
    vep_output_vds = output_vds_prefix + ".vep.vds"
    vds = vds.vep(config="/vep/vep-gcloud.properties", root='va.vep', block_size=500)
    vds.write(vep_output_vds, overwrite=True)

    input_path = vep_output_vds

hc.stop()


if args.start_with_step == 0:
    logger.info("=============================== pipeline - step 0 ===============================")
    logger.info("read in data, run vep, compute various derived fields, export to elasticsearch")

    logger.info("\n==> create HailContext")
    hc = hail.HailContext(log="/hail.log")

    vds = read_in_dataset(input_path, args.datatype, filter_interval)

    # add computed annotations
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

    #pprint(vds.variant_schema)

    # apply schema to dataset
    INPUT_SCHEMA  = {}
    if args.datatype == "GATK_VARIANTS":
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

            rsid: String,
            qual: Double,
            filters: Set[String],
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
            VQSLOD: Double,
            culprit: String,
        """
    elif args.datatype == "MANTA_SVS":
        INPUT_SCHEMA["top_level_fields"] = """
            docId: String,
            variantId: String,

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

            geneIds: Set[String],
            transcriptIds: Set[String],
            codingGeneIds: Set[String],
            transcriptConsequenceTerms: Set[String],
            sortedTranscriptConsequences: String,
            mainTranscript: Struct,
        """
        INPUT_SCHEMA["info_fields"] = """
            IMPRECISE: Boolean,
            SVTYPE: String,
            SVLEN: Array[Int],
            END: Int,
            CIPOS: Array[Int],
            CIEND: Array[Int],
            --- CIGAR: Array[String],
            --- MATEID: Array[String],
            EVENT: String,
            --- HOMLEN: Array[Int],
            --- HOMSEQ: Array[String],
            --- SVINSLEN: Array[Int],
            --- SVINSSEQ: Array[String],
            --- LEFT_SVINSSEQ: Array[String],
            --- RIGHT_SVINSSEQ: Array[String],
            --- INV3: Boolean,
            --- INV5: Boolean,
            --- BND_DEPTH: Int,
            --- MATE_BND_DEPTH: Int,
            --- JUNCTION_QUAL: Int,
        """
    else:
        raise ValueError("Unexpected datatype: %s" % args.datatype)

    expr = convert_vds_schema_string_to_annotate_variants_expr(root="va.clean", **INPUT_SCHEMA)

    vds = vds.annotate_variants_expr(expr=expr)
    vds = vds.annotate_variants_expr("va = va.clean")

    export_to_elasticsearch(
        args.host,
        args.port,
        vds,
        args.index_name,
        args.datatype,
        operation=ELASTICSEARCH_UPSERT,
        block_size=args.block_size,
        num_shards=args.num_shards,
        delete_index_before_exporting=True,
        export_genotypes=True,
        disable_doc_values_for_fields=("sortedTranscriptConsequences", ),
        disable_index_for_fields=("sortedTranscriptConsequences", ),
        export_snapshot_to_google_bucket=False,
        start_with_sample_group=args.start_with_sample_group if args.start_with_step == 0 else 0,
    )

    hc.stop()

if args.start_with_step <= 1:
    logger.info("=============================== pipeline - step 1 ===============================")
    logger.info("read in data, add in more reference datasets, export to elasticsearch")

    logger.info("\n==> create HailContext")
    hc = hail.HailContext(log="/hail.log")

    vds = read_in_dataset(input_path, args.datatype, filter_interval)
    vds = compute_minimal_schema(vds, args.datatype)

    if not args.skip_annotations and not args.exclude_cadd:
        logger.info("\n==> add cadd")
        vds = add_cadd_to_vds(hc, vds, args.genome_version, root="va.cadd", info_fields=CADD_INFO_FIELDS, subset=filter_interval)

    if not args.skip_annotations and not args.exclude_dbnsfp:
        logger.info("\n==> add dbnsfp")
        vds = add_dbnsfp_to_vds(hc, vds, args.genome_version, root="va.dbnsfp", subset=filter_interval)

    if not args.skip_annotations and not args.exclude_clinvar:
        logger.info("\n==> add clinvar")
        vds = add_clinvar_to_vds(hc, vds, args.genome_version, root="va.clinvar", info_fields=CLINVAR_INFO_FIELDS, subset=filter_interval)

    if not args.skip_annotations and not args.exclude_1kg:
        logger.info("\n==> add 1kg")
        vds = add_1kg_phase3_to_vds(hc, vds, args.genome_version, root="va.g1k", subset=filter_interval)

    if not args.skip_annotations and not args.exclude_exac:
        logger.info("\n==> add exac")
        vds = add_exac_to_vds(hc, vds, args.genome_version, root="va.exac", top_level_fields=EXAC_TOP_LEVEL_FIELDS, info_fields=EXAC_INFO_FIELDS, subset=filter_interval)

    if not args.skip_annotations and not args.exclude_topmed and args.genome_version == "38":
        logger.info("\n==> add topmed")
        vds = add_topmed_to_vds(hc, vds, args.genome_version, root="va.topmed", subset=filter_interval)

    if not args.skip_annotations and not args.exclude_mpc:
        logger.info("\n==> add mpc")
        vds = add_mpc_to_vds(hc, vds, args.genome_version, root="va.mpc", info_fields=MPC_INFO_FIELDS, subset=filter_interval)

    if not args.skip_annotations and not args.exclude_gnomad:
        logger.info("\n==> add gnomad exomes")
        vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="exomes", root="va.gnomad_exomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS, subset=filter_interval)

    if not args.skip_annotations and not args.exclude_gnomad:
        logger.info("\n==> add gnomad genomes")
        vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="genomes", root="va.gnomad_genomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS, subset=filter_interval)

    if not args.skip_annotations and not args.exclude_gnomad_coverage:
        logger.info("\n==> add gnomad coverage")
        vds = vds.persist()
        vds = add_gnomad_exome_coverage_to_vds(hc, vds, args.genome_version, root="va.gnomad_exome_coverage")
        vds = add_gnomad_genome_coverage_to_vds(hc, vds, args.genome_version, root="va.gnomad_genome_coverage")

    export_to_elasticsearch(
        args.host,
        args.port,
        vds,
        args.index_name,
        args.datatype,
        operation=ELASTICSEARCH_UPDATE,
        block_size=args.block_size,
        num_shards=args.num_shards,
        delete_index_before_exporting=False,
        export_genotypes=False,
        disable_doc_values_for_fields=(),
        disable_index_for_fields=(),
        export_snapshot_to_google_bucket=True,
        start_with_sample_group=args.start_with_sample_group if args.start_with_step == 1 else 0,
    )

    hc.stop()



    # see https://hail.is/hail/annotationdb.html#query-builder
    #final_vds = final_vds.annotate_variants_db([
    #    'va.cadd.PHRED',
    #    'va.cadd.RawScore',
    #    'va.dann.score',
    #])
