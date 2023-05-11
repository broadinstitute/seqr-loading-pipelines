#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.computed_fields.vep import (
    CONSEQUENCE_TERM_RANK_LOOKUP,
    get_expr_for_vep_sorted_transcript_consequences_array,
    get_expr_for_worst_transcript_consequence_annotations_struct,
)
from hail_scripts.reference_data.config import CONFIG, GCS_PREFIXES
from hail_scripts.utils.hail_utils import write_ht

AF_THRESHOLD = 0.9
SAMPLE_TYPE_VALIDATION_HT_PATH = (
    'sample_type_validation/sample_type_validation.GRCh{genome_version}.{version}.ht'
)
VERSION = '1.0.0'


def read_gnomad_subset(genome_version: str):
    filtered_contig = '1' if genome_version == '37' else 'chr1'
    ht = hl.read_table(CONFIG['gnomad_genomes'][genome_version]['path'])
    ht = hl.filter_intervals(
        ht,
        [
            hl.parse_locus_interval(
                filtered_contig,
                reference_genome='GRCh%s' % genome_version,
            ),
        ],
    )
    ht = ht.filter(ht.freq[0].AF > AF_THRESHOLD)
    ht = ht.annotate(
        main_transcript=(
            get_expr_for_worst_transcript_consequence_annotations_struct(
                get_expr_for_vep_sorted_transcript_consequences_array(
                    ht.vep,
                    omit_consequences=[],
                ),
            )
        ),
    )
    ht = ht.select(
        coding_variants=(
            hl.int(ht.main_transcript.major_consequence_rank)
            <= hl.int(CONSEQUENCE_TERM_RANK_LOOKUP.get('synonymous_variant'))
        ),
        noncoding_variants=(
            hl.int(ht.main_transcript.major_consequence_rank)
            >= hl.int(CONSEQUENCE_TERM_RANK_LOOKUP.get('downstream_gene_variant'))
        ),
    )
    return ht.filter(ht.coding_variants | ht.noncoding_variants)


def run(environment: str, genome_version: str):
    ht = read_gnomad_subset(genome_version)
    destination_path = os.path.join(
        GCS_PREFIXES[environment],
        SAMPLE_TYPE_VALIDATION_HT_PATH,
    ).format(
        genome_version=genome_version,
        version=VERSION,
    )
    print(f'Uploading ht to {destination_path}')
    write_ht(ht, destination_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', default='dev', choices=['dev', 'prod'])
    parser.add_argument(
        '--genome-version',
        help='Reference build, 37 or 38',
        choices=['37', '38'],
        default='38',
    )
    args, _ = parser.parse_known_args()
    run(args.environment, args.genome_version)
