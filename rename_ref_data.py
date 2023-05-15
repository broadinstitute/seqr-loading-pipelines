import hail as hl

ht = hl.read_table('gs://seqr-reference-data/GRCh37/all_reference_data/combined_reference_data_grch37.ht')
ht = ht.drop('g1k')
ht.transmute(
    gnomad_genome_coverage=hl.Struct(x10=ht.gnomad_genome_coverage),
    gnomad_exome_coverage=hl.Struct(x10=ht.gnomad_exome_coverage),
)
ht.annotate_globals(datasets=ht.datasets.remove('1kg'))
ht.write('gs://seqr-reference-data/GRCh37/all_reference_data/combined_reference_data_grch37.ht', overwrite=True)


ht = hl.read_table('gs://seqr-reference-data/GRCh38/all_reference_data/combined_reference_data_grch38.ht')
ht = ht.drop('g1k')
ht.transmute(
    gnomad_genome_coverage=hl.Struct(x10=ht.gnomad_genome_coverage),
    gnomad_exome_coverage=hl.Struct(x10=ht.gnomad_exome_coverage),
)
ht.annotate_globals(datasets={k: v for k, v in ht.datasets.collect()[0].items() if k!='1kg'})
ht.annotate_globals(version='2.0.5')
ht.write('gs://seqr-reference-data/GRCh38/all_reference_data/combined_reference_data_grch38.ht', overwrite=True)
