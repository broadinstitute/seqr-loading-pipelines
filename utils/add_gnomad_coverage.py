#!/usr/bin/env python


EXOME_COVERAGE_TSV_PATHS = {
    "37": [
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
    ],
    "38": [
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
}




COVERAGE_FIELDS = """
    chrom: String,
    pos: Int,
    x10: Float,
"""


def _import_coverage_keytable(hail_context, coverage_tsv_paths):
    from hail.expr import TInt, TDouble, TString

    field_types = {
        '#chrom': TString(), 'pos': TInt(),
        'mean': TDouble(), 'median': TDouble(),
        #'1': TDouble(), '5': TDouble(),
        '10': TDouble(),
        #'15': TDouble(), '20': TDouble(), '25': TDouble(), '30': TDouble(), '50': TDouble(), '100': TDouble(),
    }

    return (
        hail_context.import_table(coverage_tsv_paths, types=field_types)
            .rename({
            "#chrom": "chrom",
            "10": "x10",
        })
            .annotate("locus=Locus(chrom, pos)")
            .key_by('locus')
            .drop(["chrom", "pos", "mean", "median", "1", "5", "15", "20", "25", "30", "50", "100"])
    )


def add_gnomad_exome_coverage_to_vds(hail_context, vds, genome_version, root="va.gnomad_exome_coverage"):
    """Add ExAC v2 exome coverage to VDS"""

    if genome_version != "37" and genome_version != "38":
        raise ValueError("Invalid genome_version: " + str(genome_version))

    kt = _import_coverage_keytable(hail_context, EXOME_COVERAGE_TSV_PATHS[genome_version])

    return vds.annotate_variants_table(kt, root=root)


def add_gnomad_genome_coverage_to_vds(hail_context, vds, genome_version, root="va.gnomad_genome_coverage"):
    """Add gnomAD genome coverage to VDS"""

    if genome_version != "37" and genome_version != "38":
        raise ValueError("Invalid genome_version: " + str(genome_version))

    kt = _import_coverage_keytable(hail_context, GENOME_COVERAGE_TSV_PATHS[genome_version])

    return vds.annotate_variants_table(kt, root=root)

