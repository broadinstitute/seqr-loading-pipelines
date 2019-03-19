import argparse as ap
import logging

import hail as hl
from hail_scripts.v02.utils.computed_fields.vep import get_expr_for_vep_sorted_transcript_consequences_array, \
    get_expr_for_worst_transcript_consequence_annotations_struct, CONSEQUENCE_TERM_RANK_LOOKUP

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = ap.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], default="37")
args = p.parse_args()


def read_gnomad_subset(genome_version):
    logger.info("==> Read gnomAD subset")

    # select a subset of gnomad genomes variants that are in > 90% of samples
    ht = hl.split_multi(hl.read_table('gs://gnomad-public/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht'))
    ht = hl.filter_intervals(ht, [hl.parse_locus_interval('1', reference_genome='GRCh%s'%genome_version)])
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
    # ht.write(output_path, overwrite=True)


ht = read_gnomad_subset(args.genome_version)
ht.persist()

coding_ht = ht.filter(
    hl.int(ht.main_transcript.major_consequence_rank) <= hl.int(CONSEQUENCE_TERM_RANK_LOOKUP.get('synonymous_variant')))
write_out_ht(coding_ht, 'gs://seqr-kev/combined-test/validation-coding.ht')


noncoding_ht = ht.filter(
    hl.int(ht.main_transcript.major_consequence_rank) >= hl.int(CONSEQUENCE_TERM_RANK_LOOKUP.get('downstream_gene_variant')))
write_out_ht(noncoding_ht, 'gs://seqr-kev/combined-test/validation-noncoding.ht')
