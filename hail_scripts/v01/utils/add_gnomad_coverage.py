#!/usr/bin/env python


EXOME_COVERAGE_TSV_PATHS = {
    "37": [
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr1.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr10.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr11.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr12.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr13.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr14.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr15.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr16.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr17.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr18.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr19.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr2.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr20.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr21.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr22.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr3.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr4.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr5.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr6.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr7.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr8.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chr9.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chrY.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/exomes/gnomad.exomes.r2.0.1.chrX.coverage.txt.gz",
    ],
    "38": [
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
}

GENOME_COVERAGE_TSV_PATHS = {
    "37": [
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr1.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr10.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr11.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr12.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr13.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr14.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr15.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr16.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr17.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr18.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr19.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr2.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr20.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr21.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr22.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr3.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr4.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr5.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr6.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr7.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr8.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chr9.coverage.txt.gz",
        "gs://gnomad-public/release/2.0.1/coverage/genomes/gnomad.genomes.r2.0.1.chrX.coverage.txt.gz",
    ],
    "38": [
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr1.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr1.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr10.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr10.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr11.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr11.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr12.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr12.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr13.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr13.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr14.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr14.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr15.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr15.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr16.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr16.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr17.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr17.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr18.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr18.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr19.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr19.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr2.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr2.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr20.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr20.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr21.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr21.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr22.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr22.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr3.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr3.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr4.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr4.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr5.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr5.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr6.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr6.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr7.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr7.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr8.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr8.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chr9.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chr9.cov.liftover.GRCh38.txt.gz",
        "gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.genomes.r2.0.2.chrX.coverage.liftover.GRCh38.txt.gz", #"gs://seqr-reference-data/GRCh38/gnomad/coverage/gnomad.chrX.cov.liftover.GRCh38.txt.gz",
    ],
}


EXOME_COVERAGE_KEYTABLE_PATH = {
    "37": "gs://seqr-reference-data/GRCh37/gnomad/coverage/exome_coverage.kt",
    "38": "gs://seqr-reference-data/GRCh38/gnomad/coverage/exome_coverage.kt",
}

GENOME_COVERAGE_KEYTABLE_PATH = {
    "37": "gs://seqr-reference-data/GRCh37/gnomad/coverage/genome_coverage.kt",
    "38": "gs://seqr-reference-data/GRCh38/gnomad/coverage/genome_coverage.kt",
}


"""
from hail.expr import TInt, TDouble, TString

field_types = {
    '#chrom': TString(), 'pos': TInt(),
    'mean': TDouble(), 'median': TDouble(),
    #'1': TDouble(), '5': TDouble(),
    '10': TDouble(),
    #'15': TDouble(), '20': TDouble(), '25': TDouble(), '30': TDouble(), '50': TDouble(), '100': TDouble(),
}
"""


COVERAGE_FIELDS = """
    chrom: String,
    pos: Int,
    x10: Float,
"""


def _import_coverage_keytable(hail_context, keytable_path):
    return (
        hail_context.read_table(keytable_path)
            .rename({"10": "x10"})
            .drop(["#chrom", "pos", "mean", "median", "1", "5", "15", "20", "25", "30", "50", "100"])
    )


def add_gnomad_exome_coverage_to_vds(hail_context, vds, genome_version, root="va.gnomad_exome_coverage"):
    """Add ExAC v2 exome coverage to VDS"""

    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    kt = _import_coverage_keytable(hail_context, EXOME_COVERAGE_KEYTABLE_PATH[genome_version])

    return vds.annotate_variants_table(kt, root=root)


def add_gnomad_genome_coverage_to_vds(hail_context, vds, genome_version, root="va.gnomad_genome_coverage"):
    """Add gnomAD genome coverage to VDS"""

    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    kt = _import_coverage_keytable(hail_context, GENOME_COVERAGE_KEYTABLE_PATH[genome_version])

    return vds.annotate_variants_table(kt, root=root)

