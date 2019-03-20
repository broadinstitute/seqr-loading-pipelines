import argparse as ap
import logging

import hail as hl
from hail_scripts.v02.utils.computed_fields.vep import get_expr_for_vep_sorted_transcript_consequences_array, \
    get_expr_for_worst_transcript_consequence_annotations_struct, CONSEQUENCE_TERM_RANK_LOOKUP

VALIDATION_KEYTABLE_PATHS = {
    'coding_37': 'gs://seqr-reference-data/GRCh37/validate_ht/common_coding_variants.grch37.ht',
    'coding_38': 'gs://seqr-reference-data/GRCh38/validate_ht/common_coding_variants.grch38.ht',
    'noncoding_37': 'gs://seqr-reference-data/GRCh37/validate_ht/common_noncoding_variants.grch37.ht',
    'noncoding_38': 'gs://seqr-reference-data/GRCh38/validate_ht/common_noncoding_variants.grch38.ht',
}

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = ap.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], default="37")
args = p.parse_args()


def read_gnomad_subset(genome_version):
    logger.info("==> Read gnomAD subset")
    filtered_contig = '1' if genome_version == '37' else 'chr1'

    # select a subset of gnomad genomes variants that are in > 90% of samples
    ht = hl.read_table('gs://gnomad-public/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht')
    ht = hl.filter_intervals(ht, [hl.parse_locus_interval(filtered_contig,
                                                          reference_genome='GRCh%s'%genome_version)])
    ht = ht.filter(ht.freq[0].AF > 0.90)

    ht = ht.annotate(sorted_transaction_consequences=(
        get_expr_for_vep_sorted_transcript_consequences_array(ht.vep, omit_consequences=[]))
    )
    ht = ht.annotate(main_transcript=(
        get_expr_for_worst_transcript_consequence_annotations_struct(ht.sorted_transaction_consequences))
    )

    ht.describe()

    return ht


def write_out_ht(ht, output_path):
    ht = ht.select()
    print(ht.count())
    ht.write(output_path, overwrite=True)


ht = read_gnomad_subset(args.genome_version)
ht.persist()

coding_ht = ht.filter(
    hl.int(ht.main_transcript.major_consequence_rank) <= hl.int(CONSEQUENCE_TERM_RANK_LOOKUP.get('synonymous_variant')))
write_out_ht(coding_ht, VALIDATION_KEYTABLE_PATHS['coding_{}'.format(args.genome_version)])


noncoding_ht = ht.filter(
    hl.int(ht.main_transcript.major_consequence_rank) >= hl.int(CONSEQUENCE_TERM_RANK_LOOKUP.get('downstream_gene_variant')))
write_out_ht(noncoding_ht, VALIDATION_KEYTABLE_PATHS['noncoding_{}'.format(args.genome_version)])
