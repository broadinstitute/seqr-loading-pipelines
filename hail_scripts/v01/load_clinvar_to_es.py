import argparse
from pprint import pprint
from hail_scripts.v01.utils.add_clinvar import CLINVAR_GOLD_STARS_LOOKUP, CLINVAR_VDS_PATH, \
    download_and_import_latest_clinvar_vcf
from hail_scripts.v01.utils.computed_fields import get_expr_for_contig, get_expr_for_start_pos, get_expr_for_ref_allele, \
    get_expr_for_alt_allele, get_expr_for_variant_id, get_expr_for_worst_transcript_consequence_annotations_struct, \
    get_expr_for_xpos, get_expr_for_vep_gene_ids_set, get_expr_for_vep_transcript_ids_set, \
    get_expr_for_vep_sorted_transcript_consequences_array, get_expr_for_vep_protein_domains_set, \
    get_expr_for_vep_consequence_terms_set, get_expr_for_vep_transcript_id_to_consequence_map
from hail_scripts.v01.utils.elasticsearch_client import ElasticsearchClient
from hail_scripts.v01.utils.hail_utils import create_hail_context
from hail_scripts.v01.utils.vds_utils import run_vep

p = argparse.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)
p.add_argument("-H", "--host", help="Elasticsearch host or IP", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=9200, type=int)
p.add_argument("-i", "--index-name", help="Elasticsearch index name")
p.add_argument("-t", "--index-type", help="Elasticsearch index type", default="variant")
p.add_argument("-s", "--num-shards", help="Number of elasticsearch shards", default=1, type=int)
p.add_argument("--vep-block-size", help="Block size to use for VEP", default=200, type=int)
p.add_argument("--es-block-size", help="Block size to use when exporting to elasticsearch", default=200, type=int)
p.add_argument("--subset", help="Specify an interval (eg. X:12345-54321 to load a subset of clinvar")
args = p.parse_args()

client = ElasticsearchClient(
    host=args.host,
    port=args.port,
)

if args.index_name:
    index_name = args.index_name.lower()
else:
    index_name = "clinvar_grch{}".format(args.genome_version)

hc = create_hail_context()


# download vcf
vds = download_and_import_latest_clinvar_vcf(hc, args.genome_version, subset=args.subset)

# run VEP
print("\n=== Running VEP ===")
vds = run_vep(vds, root='va.vep', block_size=args.vep_block_size, genome_version=args.genome_version)

#pprint(vds.variant_schema)

computed_annotation_exprs = [
    #"va.docId = %s" % get_expr_for_variant_id(512),
    "va.chrom = %s" % get_expr_for_contig(),
    "va.pos = %s" % get_expr_for_start_pos(),
    "va.ref = %s" % get_expr_for_ref_allele(),
    "va.alt = %s" % get_expr_for_alt_allele(),
    "va.xpos = %s" % get_expr_for_xpos(pos_field="start"),

    "va.variantId = %s" % get_expr_for_variant_id(),
    "va.domains = %s" % get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root="va.vep.transcript_consequences"),
    "va.sortedTranscriptConsequences = %s" % get_expr_for_vep_sorted_transcript_consequences_array(vep_root="va.vep"),
    "va.transcriptConsequenceTerms = %s" % get_expr_for_vep_consequence_terms_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
    "va.transcriptIds = %s" % get_expr_for_vep_transcript_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
    "va.transcriptIdToConsequenceMap = %s" % get_expr_for_vep_transcript_id_to_consequence_map(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
    "va.geneIds = %s" % get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
    "va.mainTranscript = %s" % get_expr_for_worst_transcript_consequence_annotations_struct("va.sortedTranscriptConsequences"),
    #"va.codingGeneIds = %s" % get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences", only_coding_genes=True, exclude_upstream_downstream_genes=True),
    #"va.sortedTranscriptConsequences = json(va.sortedTranscriptConsequences)",
]

for expr in computed_annotation_exprs:
    vds = vds.annotate_variants_expr(expr)

expr = """
    va.clean.variant_id = va.variantId,

    va.clean.chrom = va.chrom,
    va.clean.pos = va.pos,
    va.clean.ref = va.ref,

    va.clean.xpos = va.xpos,

    va.clean.transcript_consequence_terms = va.transcriptConsequenceTerms,
    va.clean.domains = va.domains,
    va.clean.transcript_ids = va.transcriptIds,
    va.clean.gene_ids = va.geneIds,
    va.clean.transcript_id_to_consequence_json = va.transcriptIdToConsequenceMap,
    va.clean.main_transcript = va.mainTranscript,
    va.clean.allele_id = va.info.ALLELEID,
    va.clean.clinical_significance = va.info.CLNSIG.toSet.mkString(","),
    va.clean.gold_stars = {}.get(va.info.CLNREVSTAT.toSet.mkString(","))
""".format(CLINVAR_GOLD_STARS_LOOKUP)

vds = vds.annotate_variants_expr(expr=expr)
vds = vds.annotate_variants_expr("va = va.clean")

pprint(vds.variant_schema)

summary = vds.summarize()
print("\nSummary:" + str(summary))

print("Export to elasticsearch")
client.export_vds_to_elasticsearch(
    vds,
    index_name=index_name,
    index_type_name=args.index_type,
    block_size=args.es_block_size,
    num_shards=args.num_shards,
    delete_index_before_exporting=True,
    #elasticsearch_mapping_id="doc_id",
    is_split_vds=True,
    verbose=True,
    export_globals_to_index_meta=True,
)

