import argparse as ap
import hail
import logging
from pprint import pprint

from hail_scripts.v01.utils.add_gnomad import GNOMAD_SEQR_VDS_PATHS
from hail_scripts.v01.utils.computed_fields import get_expr_for_vep_sorted_transcript_consequences_array, \
    get_expr_for_worst_transcript_consequence_annotations_struct, CONSEQUENCE_TERM_RANK_LOOKUP
from hail_scripts.v01.utils.validate_vds import VALIDATION_KEYTABLE_PATHS
from hail_scripts.v01.utils.vds_utils import read_vds, run_vep

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = ap.ArgumentParser()
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)
args = p.parse_args()

hc = hail.HailContext(log="/hail.log")


def read_gnomad_subset(genome_version):
    logger.info("==> Read gnomAD subset")

    # select a subset of gnomad genomes variants that are in > 99% of samples
    vds = (
        read_vds(hc, GNOMAD_SEQR_VDS_PATHS["genomes_" + genome_version])
            .filter_intervals(hail.Interval.parse("1"))
            .split_multi()
            .filter_variants_expr('va.info.AF[va.aIndex-1] > 0.90', keep=True)
    )

    if genome_version == "38":
        # lifted-over gnomad vds doesn't have VEP annotations
        vds = run_vep(vds, genome_version=genome_version)

    computed_annotation_exprs = [
        "va.sortedTranscriptConsequences = %s" % get_expr_for_vep_sorted_transcript_consequences_array(vep_root="va.vep"),
        "va.mainTranscript = %s" % get_expr_for_worst_transcript_consequence_annotations_struct("va.sortedTranscriptConsequences"),
    ]

    for expr in computed_annotation_exprs:
        vds = vds.annotate_variants_expr(expr)

    pprint(vds.variant_schema)
    logger.info(vds.summarize())

    return vds


def write_out_keytable(vds, output_path):
    vds = vds.annotate_variants_expr('va = {}')    # drop all variant-level fields except chrom-pos-ref-alt
    kt = vds.make_table('v = v', []).key_by('v')

    logger.info("\n==> write out: " + output_path)
    logger.info(vds.summarize())
    kt.write(output_path, overwrite=True)


vds = read_gnomad_subset(args.genome_version)

vds = vds.persist()

coding_vds = vds.filter_variants_expr(
    'va.mainTranscript.major_consequence_rank.toInt <= {}.get("synonymous_variant").toInt'.format(CONSEQUENCE_TERM_RANK_LOOKUP))
write_out_keytable(coding_vds, VALIDATION_KEYTABLE_PATHS['coding_{}'.format(args.genome_version)])


noncoding_vds = vds.filter_variants_expr(
    'va.mainTranscript.major_consequence_rank.toInt >= {}.get("downstream_gene_variant").toInt'.format(CONSEQUENCE_TERM_RANK_LOOKUP))
write_out_keytable(noncoding_vds, VALIDATION_KEYTABLE_PATHS['noncoding_{}'.format(args.genome_version)])
