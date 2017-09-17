#!/usr/bin/env python

import argparse
import hail
from hail.expr import TInt, TDouble, TString
from pprint import pprint
from utils.elasticsearch_utils import export_kt_to_elasticsearch

p = argparse.ArgumentParser()
p.add_argument("-s", "--num-shards", help="Number of shards", default=1, type=int)

# parse args
args = p.parse_args()

hc = hail.HailContext(log="/hail.log") #, branching_factor=1)

COVERAGE_TSV_PATHS = {
    "37": {
        "exomes": [
            "gs://gnomad-browser/exomes/coverage/exacv2.chr1.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr10.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr11.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr12.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr13.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr14.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr15.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr16.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr17.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr18.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr19.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr2.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr20.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr21.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr22.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr3.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr4.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr5.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr6.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr7.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr8.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chr9.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chrY.cov.txt.gz",
            "gs://gnomad-browser/exomes/coverage/exacv2.chrX.cov.txt.gz",
        ],
        "genomes": [
            "gs://gnomad-browser/genomes/coverage/Panel.chr1.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr10.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr11.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr12.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr13.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr14.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr15.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr16.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr17.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr18.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr19.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr2.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr20.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr21.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr22.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr3.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr4.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr5.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr6.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr7.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr8.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chr9.genome.coverage.txt.gz",
            "gs://gnomad-browser/genomes/coverage/Panel.chrX.genome.coverage.txt.gz",
        ]
    },
    "38": {
        "exomes": [
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr1.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr10.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr11.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr12.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr13.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr14.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr15.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr16.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr17.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr18.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr19.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr2.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr20.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr21.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr22.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr3.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr4.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr5.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr6.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr7.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr8.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chr9.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chrX.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/exacv2.chrY.cov.liftover.GRCh38.txt.gz",
        ],
        "genomes": [
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr1.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr10.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr11.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr12.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr13.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr14.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr15.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr16.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr17.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr18.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr19.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr2.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr20.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr21.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr22.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr3.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr4.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr5.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr6.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr7.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr8.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr9.cov.liftover.GRCh38.txt.gz",
            "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chrX.cov.liftover.GRCh38.txt.gz",
        ],
    },
}



types = {
    '#chrom': TString(),
    'pos': TInt(),
    'mean': TDouble(),
    'median': TDouble(),
    '1': TDouble(),
    '5': TDouble(),
    '10': TDouble(),
    '15': TDouble(),
    '20': TDouble(),
    '25': TDouble(),
    '30': TDouble(),
    '50': TDouble(),
    '100': TDouble()
}

if args.coverage_type == 'exome':
    COVERAGE_PATHS = EXOME_COVERAGE_CSV_PATHS
if args.coverage_type == 'genome':
    COVERAGE_PATHS = GENOME_COVERAGE_CSV_PATHS
if args.coverage_type == 'test':
    # x chromosome only
    COVERAGE_PATHS = EXOME_COVERAGE_CSV_PATHS[-1]

kt_coverage = hc.import_table(COVERAGE_PATHS, types=types).rename({
    '#chrom': 'chrom',
    '1': 'over1',
    '5': 'over5',
    '10': 'over10',
    '15': 'over15',
    '20': 'over20',
    '25': 'over25',
    '30': 'over30',
    '50': 'over50',
    '100': 'over100',
})

