#!/usr/bin/env python

import argparse
import hail
import os
from pprint import pprint

from hail_scripts.utils.computed_fields import get_expr_for_contig, get_expr_for_start_pos, get_expr_for_ref_allele, \
    get_expr_for_alt_allele, get_expr_for_variant_id, get_expr_for_worst_transcript_consequence_annotations_struct, \
    get_expr_for_xpos, get_expr_for_vep_gene_ids_set, get_expr_for_vep_transcript_ids_set, \
    get_expr_for_vep_sorted_transcript_consequences_array, get_expr_for_vep_protein_domains_set, \
    get_expr_for_vep_consequence_terms_set
from hail_scripts.utils.elasticsearch_client import ElasticsearchClient

p = argparse.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)
p.add_argument("-H", "--host", help="Elasticsearch host or IP", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=9200, type=int)
p.add_argument("-i", "--index-name", help="Elasticsearch index name")
p.add_argument("-t", "--index-type", help="Elasticsearch index type", default="variant")
p.add_argument("-s", "--num-shards", help="Number of elasticsearch shards", default=1, type=int)
p.add_argument("-b", "--block-size", help="Elasticsearch block size to use when exporting", default=200, type=int)

# parse args
args = p.parse_args()

client = ElasticsearchClient(
    host=args.host,
    port=args.port,
)

if args.index_name:
    index_name = args.index_name.lower()
else:
    index_name = "clinvar_grch{}".format(args.genome_version)

hc = hail.HailContext(log="/hail.log")


def run(command):
    print(command)
    os.system(command)

# download vcf
print("Downloading clinvar vcf")
run("wget ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh{0}/clinvar.vcf.gz -O /tmp/clinvar.vcf.gz".format(args.genome_version))
run("ls -l /tmp/clinvar.vcf.gz")
run("hdfs dfs -cp file:///tmp/clinvar.vcf.gz /")

# import vcf into hail
vds = hc.import_vcf("/clinvar.vcf.gz", force=True).filter_intervals(hail.Interval.parse("1-MT"))  # 22:1-25000000"))
vds = vds.repartition(2000)

# handle multi-allelics
if vds.was_split():
    vds = vds.annotate_variants_expr('va.aIndex = 1, va.wasSplit = false')
else:
    vds = vds.split_multi()

# for some reason, this additional filter is necessary to avoid
#  IllegalArgumentException: requirement failed: called altAllele on a non-biallelic variant
vds = vds.filter_variants_expr("v.isBiallelic()", keep=True)


# run VEP
vds = vds.vep(config="/vep/vep-gcloud.properties", root='va.vep', block_size=1000)

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
    "va.geneIds = %s" % get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences", exclude_upstream_downstream_genes=True),
    #"va.codingGeneIds = %s" % get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences", only_coding_genes=True, exclude_upstream_downstream_genes=True),
    "va.mainTranscript = %s" % get_expr_for_worst_transcript_consequence_annotations_struct("va.sortedTranscriptConsequences"),
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
    va.clean.main_transcript = va.mainTranscript,
    
    va.clean.allele_id = va.info.ALLELEID,
    va.clean.clinical_significance = va.info.CLNSIG.toSet.mkString(","),
    va.clean.review_status = va.info.CLNREVSTAT.toSet.mkString(",")
"""

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
    block_size=args.block_size,
    num_shards=args.num_shards,
    delete_index_before_exporting=True,
    #elasticsearch_mapping_id="doc_id",
    is_split_vds=True,
    verbose=True,
)
