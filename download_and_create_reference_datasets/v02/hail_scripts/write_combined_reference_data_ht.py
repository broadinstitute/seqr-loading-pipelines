import argparse
import os

import hail as hl

from hail_scripts.reference_data.combine import join_hts
from hail_scripts.reference_data.config import CONFIG

VERSION = '2.0.5'
OUTPUT_TEMPLATE = 'gs://seqr-reference-data/GRCh{genome_version}/' \
                  'all_reference_data/v2/combined_reference_data_grch{genome_version}-{version}.ht'

def run(args):
    hl._set_flags(no_whole_stage_codegen='1')  # hail 0.2.78 hits an error on the join, this flag gets around it
    joined_ht = join_hts(['cadd', 'tgp', 'mpc', 'eigen', 'dbnsfp', 'topmed', 'primate_ai', 'splice_ai', 'exac',
              'gnomad_genomes', 'gnomad_exomes', 'geno2mp', 'gnomad_genome_coverage', 'gnomad_exome_coverage'],
              VERSION,
             args.build,)
    output_path = os.path.join(OUTPUT_TEMPLATE.format(genome_version=args.build, version=VERSION))
    print('Writing to %s' % output_path)
    joined_ht.write(os.path.join(output_path))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--build', help='Reference build, 37 or 38', choices=["37", "38"], required=True)
    args = parser.parse_args()

    run(args)
