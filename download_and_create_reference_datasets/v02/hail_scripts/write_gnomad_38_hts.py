#!/usr/bin/env python3

# Add gnomAD's site only HT globals and row annotations to the 38 liftover

import logging
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


import hail as hl
from hail_scripts.v02.utils.hail_utils import write_ht, import_table

hl.init()

CONFIG = {
    'exomes': {
        '37': 'gs://gnomad-public/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht',
        '38': 'gs://gnomad-public/release/2.1.1/liftover_grch38/ht/exomes/gnomad.exomes.r2.1.1.sites.liftover_grch38.ht',
        'output_path': 'gs://seqr-reference-data/GRCh38/gnomad/gnomad.exomes.r2.1.1.sites.liftover_grch38.ht'
    },
    'genomes': {
        '37': 'gs://gnomad-public/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht',
        '38': 'gs://gnomad-public/release/2.1.1/liftover_grch38/ht/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht',
        'output_path': 'gs://seqr-reference-data/GRCh38/gnomad/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht'
    }
}

def liftover_annotations(gnomad_37_path, gnomad_38_path, annotated_gnomad_38_path):
    """
    The 38 liftover of gnomAD is stripped of all global and row annotations. This function
    annotates the 38 liftover with the original 37 annotations as the combined reference
    data script needs them.
    :param gnomad_37_path: path to 37 version of gnomAD for data type
    :param gnomad_38_path: path to 38 version of gnomAD for data type
    :param annotated_gnomad_38_path: path to annotated 38 version of gnomAD for data type
    :return:
    """
    ht_37 = hl.read_table(gnomad_37_path)
    ht_38 = hl.read_table(gnomad_38_path)
    ht_38 = ht_38.annotate(original_alleles=hl.or_else(ht_38.original_alleles, ht_38.alleles))
    ht_38 = ht_38.key_by('original_locus', 'original_alleles')
    ht_38 = ht_38.annotate(**ht_37[ht_38.key])
    ht_38 = ht_38.annotate_globals(**ht_37.index_globals())
    ht_38 = ht_38.key_by('locus', 'alleles')
    ht_38.write(annotated_gnomad_38_path, overwrite=True)
    return ht_38

def run():
    for data_type, config in CONFIG.items():
        ht = liftover_annotations(config["37"], config["38"], config['output_path'])
        ht.describe()

run()
