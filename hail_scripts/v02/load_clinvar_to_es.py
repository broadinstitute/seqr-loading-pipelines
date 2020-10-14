import argparse

import hail as hl

from hail_scripts.v02.utils.clinvar import CLINVAR_GOLD_STARS_LOOKUP, download_and_import_latest_clinvar_vcf
from hail_scripts.v02.utils.computed_fields import (
    get_expr_for_alt_allele,
    get_expr_for_contig,
    get_expr_for_ref_allele,
    get_expr_for_start_pos,
    get_expr_for_variant_id,
    get_expr_for_xpos,
    get_expr_for_vep_consequence_terms_set,
    get_expr_for_vep_gene_id_to_consequence_map,
    get_expr_for_vep_gene_ids_set,
    get_expr_for_vep_protein_domains_set,
    get_expr_for_vep_sorted_transcript_consequences_array,
    get_expr_for_vep_transcript_id_to_consequence_map,
    get_expr_for_vep_transcript_ids_set,
    get_expr_for_worst_transcript_consequence_annotations_struct,
)
from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient


p = argparse.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)
p.add_argument("-H", "--host", help="Elasticsearch host or IP", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=9200, type=int)
p.add_argument("-i", "--index-name", help="Elasticsearch index name")
p.add_argument("-t", "--index-type", help="Elasticsearch index type", default="variant")
p.add_argument("-s", "--num-shards", help="Number of elasticsearch shards", default=1, type=int)
p.add_argument("-b", "--es-block-size", help="Elasticsearch block size to use when exporting", default=200, type=int)
args = p.parse_args()


if args.index_name:
    index_name = args.index_name.lower()
else:
    index_name = "clinvar_grch{}".format(args.genome_version)


print("\n=== Downloading VCF ===")
mt = download_and_import_latest_clinvar_vcf(args.genome_version)
print(dict(mt.globals.value))

print("\n=== Running VEP ===")
mt = hl.vep(mt, "file:///vep/vep85-gcloud.json", name="vep", block_size=1000)

print("\n=== Processing ===")
mt = mt.annotate_rows(
    sortedTranscriptConsequences=get_expr_for_vep_sorted_transcript_consequences_array(vep_root=mt.vep)
)

mt = mt.annotate_rows(
    main_transcript=get_expr_for_worst_transcript_consequence_annotations_struct(
        vep_sorted_transcript_consequences_root=mt.sortedTranscriptConsequences
    )
)

mt = mt.annotate_rows(
    gene_ids=get_expr_for_vep_gene_ids_set(
        vep_transcript_consequences_root=mt.sortedTranscriptConsequences
    ),
)

review_status_str = hl.delimit(hl.sorted(hl.array(hl.set(mt.info.CLNREVSTAT)), key=lambda s: s.replace("^_", "z")))

mt = mt.select_rows(
    allele_id=mt.info.ALLELEID,
    alt=get_expr_for_alt_allele(mt),
    chrom=get_expr_for_contig(mt.locus),
    clinical_significance=hl.delimit(hl.sorted(hl.array(hl.set(mt.info.CLNSIG)), key=lambda s: s.replace("^_", "z"))),
    domains=get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root=mt.vep.transcript_consequences),
    gene_ids=mt.gene_ids,
    gene_id_to_consequence_json=get_expr_for_vep_gene_id_to_consequence_map(
        vep_sorted_transcript_consequences_root=mt.sortedTranscriptConsequences,
        gene_ids=mt.gene_ids
    ),
    gold_stars=CLINVAR_GOLD_STARS_LOOKUP[review_status_str],
    **{f"main_transcript_{field}": mt.main_transcript[field] for field in mt.main_transcript.dtype.fields},
    pos=get_expr_for_start_pos(mt),
    ref=get_expr_for_ref_allele(mt),
    review_status=review_status_str,
    transcript_consequence_terms=get_expr_for_vep_consequence_terms_set(
        vep_transcript_consequences_root=mt.sortedTranscriptConsequences
    ),
    transcript_ids=get_expr_for_vep_transcript_ids_set(
        vep_transcript_consequences_root=mt.sortedTranscriptConsequences
    ),
    transcript_id_to_consequence_json=get_expr_for_vep_transcript_id_to_consequence_map(
        vep_transcript_consequences_root=mt.sortedTranscriptConsequences
    ),
    variant_id=get_expr_for_variant_id(mt),
    xpos=get_expr_for_xpos(mt.locus),
)

print("\n=== Summary ===")
hl.summarize_variants(mt)

# Drop key columns for export
rows = mt.rows()
rows = rows.order_by(rows.variant_id).drop("locus", "alleles")

print("\n=== Exporting to Elasticsearch ===")
es = ElasticsearchClient(args.host, args.port)
es.export_table_to_elasticsearch(
    rows,
    index_name=index_name,
    index_type_name=args.index_type,
    block_size=args.es_block_size,
    num_shards=args.num_shards,
    delete_index_before_exporting=True,
    export_globals_to_index_meta=True,
    verbose=True,
)
