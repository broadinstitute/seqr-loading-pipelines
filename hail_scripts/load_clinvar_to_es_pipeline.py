#!/usr/bin/env python

import argparse
import hail
import os
from pprint import pprint

from hail_scripts.utils.computed_fields import get_expr_for_contig, get_expr_for_start_pos, get_expr_for_ref_allele, \
    get_expr_for_alt_allele, get_expr_for_variant_id
from hail_scripts.utils.elasticsearch_client import ElasticsearchClient

p = argparse.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)
p.add_argument("-H", "--host", help="Elasticsearch node host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=30001, type=int)  # 9200
p.add_argument("-i", "--index", help="Elasticsearch index name", default="clinvar")
p.add_argument("-t", "--index-type", help="Elasticsearch index type", default="variant")
#p.add_argument("-v", "--clinvar-vds", help="Path to clinvar data", required=True)
p.add_argument("-b", "--block-size", help="Elasticsearch block size", default=200, type=int)
p.add_argument("-s", "--num-shards", help="Number of shards", default=1, type=int)

# parse args
args = p.parse_args()

es = ElasticsearchClient(
    host=args.host,
    port=args.port,
)

hc = hail.HailContext(log="/hail.log") #, branching_factor=1)


def run(command):
    print(command)
    os.system(command)


run("wget ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh{0}/clinvar.vcf.gz -O /tmp/clinvar.vcf.gz".format(args.genome_version))
run("ls -l /tmp/clinvar.vcf.gz")
run("hdfs dfs -cp file:///tmp/clinvar.vcf.gz /")

vds = hc.import_vcf("/clinvar.vcf.gz", force=True)
vds = vds.repartition(2000)

vds = vds.vep(config="/vep/vep-gcloud.properties", root='va.vep', block_size=1000)

pprint(vds.variant_schema)


"""
          chrom: elastic_variant.chrom,
          pos: elastic_variant.pos,
          xpos: elastic_variant.xpos,
          ref: elastic_variant.ref,
          alt: elastic_variant.alt,
          variant_id: `${elastic_variant.chrom}-${elastic_variant.pos}-${elastic_variant.ref}-${elastic_variant.alt}`,
          measureset_type: elastic_variant.MEASURESET_TYPE,
          measureset_id: elastic_variant.MEASURESET_ID,
          // rcv: elastic_variant.RCV,
          allele_id: elastic_variant.ALLELE_ID,
          symbol: elastic_variant.SYMBOL,
          hgvsc: elastic_variant.HGVS_C,
          hgvsp: elastic_variant.HGVS_P,
          molecular_consequence: elastic_variant.MOLECULAR_CONSEQUENCE,
          clinical_significance: elastic_variant.CLINICAL_SIGNIFICANCE,
          pathogenic: elastic_variant.PATHOGENIC,
          benign: elastic_variant.BENIGN,
          inflicted: elastic_variant.CONFLICTED,
          review_status: elastic_variant.REVIEW_STATUS,
          gold_stars: elastic_variant.GOLD_STARS,
          all_submitters: elastic_variant.ALL_SUBMITTERS,
          all_traits: elastic_variant.ALL_TRAITS,
          all_pmids: elastic_variant.ALL_PMIDS,
          inheritance_modes: elastic_variant.INHERITANCE_MODES,
          age_of_onset: elastic_variant.AGE_OF_ONSET,
          prevalence: elastic_variant.PREVALENCE,
          disease_mechanism: elastic_variant.DISEASE_MECHANISM,
          origin: elastic_variant.ORIGIN,
"""
parallel_computed_annotation_exprs = [
    "va.variantId = %s" % get_expr_for_variant_id(),

    "va.chrom = %s" % get_expr_for_contig(),
    "va.pos = %s" % get_expr_for_start_pos(),
    "va.ref = %s" % get_expr_for_ref_allele(),
    "va.alt = %s" % get_expr_for_alt_allele(),

    # compute AC, Het, Hom, Hemi, AN
    "va.xpos = %s" % get_expr_for_xpos(pos_field="start"),

    "va.geneIds = %s" % get_expr_for_vep_gene_ids_set(vep_root="va.vep"),
    "va.codingGeneIds = %s" % get_expr_for_vep_gene_ids_set(vep_root="va.vep", only_coding_genes=True),
    "va.transcriptIds = %s" % get_expr_for_vep_transcript_ids_set(vep_root="va.vep"),
    "va.domains = %s" % get_expr_for_vep_protein_domains_set(vep_root="va.vep"),
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

# based on output of pprint(vds.variant_schema)
CLINVAR_SCHEMA = {
    "top_level_fields": """
        chrom: String,
        pos: Int,
        xpos: Int,
        ref: String,
        alt: String,
    """,

    "info_fields": """
         --- AF_ESP: Double,
         --- AF_EXAC: Double,
         --- AF_TGP: Double,
         ALLELEID: Int,
         --- CLNDN: Array[String],
         --- CLNDNINCL: Array[String],
         --- CLNDISDB: Array[String],
         --- CLNDISDBINCL: Array[String],
         --- CLNHGVS: Array[String],
         CLNREVSTAT: Array[String],
         CLNSIG: Array[String],
         --- CLNSIGCONF: Array[String],
         --- CLNSIGINCL: Array[String],
         --- CLNVC: String,
         --- CLNVCSO: String,
         --- CLNVI: Array[String],
         --- DBVARID: Array[String],
         --- GENEINFO: String,
         --- MC: Array[String],
         --- ORIGIN: Array[String],
         --- RS: Array[String],
         --- SSR: Int
    """
}

vds_computed_annotations_exprs = [
    "va.chrom = %s" % get_expr_for_contig(),
    "va.pos = %s" % get_expr_for_start_pos(),
    "va.ref = %s" % get_expr_for_ref_allele(),
    "va.alt = %s" % get_expr_for_alt_allele(),
    "va.xpos = %s" % get_expr_for_xpos(),
]

print("======== Exomes: KT Schema ========")
for expr in vds_computed_annotations_exprs:
    vds = vds.annotate_variants_expr(expr)
kt_variant_expr = convert_vds_schema_string_to_vds_make_table_arg(split_multi=False, **SCHIZOPHRENIA_SCHEMA)
kt = vds.make_table(kt_variant_expr, [])

pprint(kt.schema)

# DISABLE_INDEX_AND_DOC_VALUES_FOR_FIELDS = ("sortedTranscriptConsequences", )

print("======== Export to elasticsearch ======")
es.export_kt_to_elasticsearch(
    kt,
    index_name=args.index,
    index_type_name=args.index_type,
    block_size=args.block_size,
    num_shards=args.num_shards,
    delete_index_before_exporting=True,
    verbose=True,
)
