#!/usr/bin/env python

import argparse
import hail
import os
from pprint import pprint

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
export_kt_to_elasticsearch(
    kt,
    host=args.host,
    port=args.port,
    index_name=args.index,
    index_type_name=args.index_type,
    block_size=args.block_size,
    num_shards=args.num_shards,
    delete_index_before_exporting=True,
    # disable_doc_values_for_fields=DISABLE_INDEX_AND_DOC_VALUES_FOR_FIELDS,
    # disable_index_for_fields=DISABLE_INDEX_AND_DOC_VALUES_FOR_FIELDS,
    verbose=True,
)