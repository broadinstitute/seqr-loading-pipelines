import argparse
import hail
from pprint import pprint
from utils.computed_fields_utils import get_expr_for_variant_id, \
    get_expr_for_vep_gene_ids_set, get_expr_for_vep_transcript_ids_set, \
    get_expr_for_orig_alt_alleles_set, get_expr_for_vep_consequence_terms_set, \
    get_expr_for_vep_sorted_transcript_consequences_array, \
    get_expr_for_worst_transcript_consequence_annotations_struct, get_expr_for_end_pos, \
    get_expr_for_xpos, get_expr_for_contig, get_expr_for_start_pos, get_expr_for_alt_allele, \
    get_expr_for_ref_allele
from utils.gcloud_utils import inputs_older_than_outputs
from utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr
from utils.add_1kg_phase3 import add_1kg_phase3_from_vds
from utils.add_cadd import add_cadd_from_vds
from utils.add_clinvar import add_clinvar_from_vds
from utils.add_exac import add_exac_from_vds
from utils.add_gnomad import add_gnomad_from_vds
from utils.add_mpc import add_mpc_from_vds
from utils.elasticsearch_utils import export_vds_to_elasticsearch

p = argparse.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True )
p.add_argument("-f", "--force-vep", help="Re-run VEP even if the input file is already annotated.")
p.add_argument("--force-annotations", help="Re-run the step where other reference datasets are added.")
p.add_argument("-H", "--host", help="Elasticsearch node host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=30001, type=int)
p.add_argument("-i", "--index", help="Elasticsearch index name", default="variant_callset")
p.add_argument("-t", "--index-type", help="Elasticsearch index type", default="variant")
p.add_argument("-b", "--block-size", help="Elasticsearch block size", default=5000)
p.add_argument("dataset_path", help="input VCF or VDS")

# parse args
args = p.parse_args()

print("\n==> create HailContext")
hc = hail.HailContext(log="/hail.log")

input_dataset_path = args.dataset_path
input_dataset_path_prefix = input_dataset_path.replace(".vds", "").replace(".vcf", "").replace(".gz", "").replace(".bgz", "")

vep_annotations_vds_path = input_dataset_path_prefix + ".vep.vds"
vep_and_other_annotations_vds_path = input_dataset_path_prefix + ".vep_and_other_annotations.vds"

if inputs_older_than_outputs([input_dataset_path], [vep_and_other_annotations_vds_path]):
    print("\n==> import vep-and-other-annotations vds: " + str(vep_and_other_annotations_vds_path))
    vds_type = "WITH_VEP_AND_OTHER_ANNOTATIONS"
    vds = hc.read(vep_and_other_annotations_vds_path)
elif inputs_older_than_outputs([input_dataset_path], [vep_annotations_vds_path]):
    print("\n==> import vep-annotations vds: " + str(vep_annotations_vds_path))
    vds_type = "WITH_VEP_ANNOTATIONS"
    vds = hc.read(vep_annotations_vds_path)
else:
    vds_type = "NEEDS_ALL_ANNOTATIONS"
    if input_dataset_path.endswith(".vds"):
        print("\n==> import vds: " + str(input_dataset_path))
        vds = hc.read(input_dataset_path)
    else:
        print("\n==> import vcf: " + str(input_dataset_path))
        vds = hc.import_vcf(input_dataset_path, force_bgz=True, min_partitions=1000)

    print("\n==> set originalAltAlleles")
    vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())

    print("\n==> split_multi()")
    vds = vds.split_multi()

print("\n==> print schema")
pprint(vds.variant_schema)

if args.force_vep or vds_type in ("NEEDS_ALL_ANNOTATIONS",):
    print("\n==> running VEP: " + str(vep_annotations_vds_path))
    vds = vds.vep(config="/vep/vep-gcloud.properties", root='va.vep', block_size=1000)  #, csq=True)
    vds.write(vep_annotations_vds_path, overwrite=True)

    print("\n==> print schema with VEP")
    pprint(vds.variant_schema)

if args.force_annotations or vds_type in ("NEEDS_ALL_ANNOTATIONS", "WITH_VEP_ANNOTATIONS"):
    print("\n==> adding other annotations: " + str(vep_and_other_annotations_vds_path))

    vds_computed_annotations_exprs = [
        "va.geneIds = %s" % get_expr_for_vep_gene_ids_set(vep_root="va.vep"),
        "va.transcriptIds = %s" % get_expr_for_vep_transcript_ids_set(vep_root="va.vep"),
        "va.transcriptConsequenceTerms = %s" % get_expr_for_vep_consequence_terms_set(vep_root="va.vep"),
        "va.sortedTranscriptConsequences = %s" % get_expr_for_vep_sorted_transcript_consequences_array(vep_root="va.vep"),
        "va.mainTranscript = %s" % get_expr_for_worst_transcript_consequence_annotations_struct("va.sortedTranscriptConsequences"),
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

    print("\n==> annotate variants expr")
    for expr in vds_computed_annotations_exprs:
        vds = vds.annotate_variants_expr(expr)

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
             BaseQRankSum: Double,
             ClippingRankSum: Double,
             DP: Int,
             END: Int,
             FS: Double,
             InbreedingCoeff: Double,
             MQ: Double,
             MQRankSum: Double,
             QD: Double,
             ReadPosRankSum: Double,
             VQSLOD: Double,
             culprit: String,
        """
    }

    vds = vds.annotate_variants_expr(
        convert_vds_schema_string_to_annotate_variants_expr(root="va.clean", **INPUT_SCHEMA)
    )
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
        AN_AFR: Int,
        AN_AMR: Int,
        AN_EAS: Int,
        AN_FIN: Int,
        AN_NFE: Int,
        AN_OTH: Int,
        AN_SAS: Int,
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
        POPMAX: Array[String],
        AF_POPMAX: Array[Double],
    """

    #print("\n==> add clinvar")
    #vds = add_clinvar_from_vds(hc, vds, args.genome_version, root="va.clinvar", info_fields=CLINVAR_INFO_FIELDS)
    #print("\n==> add cadd")
    #vds = add_cadd_from_vds(hc, vds, args.genome_version, root="va.cadd", info_fields=CADD_INFO_FIELDS)
    print("\n==> add mpc ---")
    vds = add_mpc_from_vds(hc, vds, args.genome_version, root="va.mpc", info_fields=MPC_INFO_FIELDS)
    print("SCHEMA MPC: ")
    pprint(vds.variant_schema)
    #print("\n==> add 1kg")
    #vds = add_1kg_phase3_from_vds(hc, vds, args.genome_version, root="va.g1k")
    #print("\n==> add exac")
    #vds = add_exac_from_vds(hc, vds, args.genome_version, root="va.exac", top_level_fields=EXAC_TOP_LEVEL_FIELDS, info_fields=EXAC_INFO_FIELDS)
    #print("\n==> add gnomad exomes")
    #vds = add_gnomad_from_vds(hc, vds, args.genome_version, exomes_or_genomes="exomes", root="va.gnomad_exomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS)
    #print("\n==> add gnomad genomes")
    #vds = add_gnomad_from_vds(hc, vds, args.genome_version, exomes_or_genomes="genomes", root="va.gnomad_genomes", top_level_fields=GNOMAD_TOP_LEVEL_FIELDS, info_fields=GNOMAD_INFO_FIELDS)

    vds.write(vep_and_other_annotations_vds_path, overwrite=True)

    # see https://hail.is/hail/annotationdb.html#query-builder
    #vds = vds.annotate_variants_db([
    #    'va.cadd.PHRED',
    #    'va.cadd.RawScore',
    #    'va.dann.score',
    #])

pprint(vds.summarize())
pprint(vds.variant_schema)
pprint(vds.sample_ids)

MAX_SAMPLES_PER_INDEX = 150
NUM_INDEXES = 1 + (len(vds.sample_ids) - 1)/MAX_SAMPLES_PER_INDEX
for i in range(NUM_INDEXES):
    index_name = "%s_%s" % (args.index, i)
    print("\n==> load samples %s to %s of %s samples into %s" % (i*MAX_SAMPLES_PER_INDEX, (i+1)*MAX_SAMPLES_PER_INDEX, len(vds.sample_ids), index_name))

    vds_sample_subset = vds.filter_samples_list(vds.sample_ids[i*MAX_SAMPLES_PER_INDEX:(i+1)*MAX_SAMPLES_PER_INDEX], keep=True)
    print("\n==> export to elasticsearch")
    DISABLE_INDEX_FOR_FIELDS = ("sortedTranscriptConsequences", )
    DISABLE_DOC_VALUES_FOR_FIELDS = ("sortedTranscriptConsequences", )

    export_vds_to_elasticsearch(
        vds_sample_subset,
        export_genotypes=True,
        host=args.host,
        port=args.port,
        index_name=index_name,
        index_type_name=args.index_type,
        block_size=args.block_size,
        delete_index_before_exporting=True,
        disable_doc_values_for_fields=DISABLE_DOC_VALUES_FOR_FIELDS,
        disable_index_for_fields=DISABLE_INDEX_FOR_FIELDS,
        is_split_vds=True,
        verbose=True,
    )

