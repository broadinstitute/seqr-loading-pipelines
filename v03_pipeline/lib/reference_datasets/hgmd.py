import hail as hl

from v03_pipeline.lib.core import ReferenceGenome


def get_ht(path: str, reference_genome: ReferenceGenome) -> hl.Table:
    mt = hl.import_vcf(
        path,
        reference_genome=reference_genome.value,
        force=True,
        skip_invalid_loci=True,
        contig_recoding=reference_genome.contig_recoding(),
    )
    ht = mt.rows()
    # HGMD represents DELETIONS as Structural Variants
    # which is less than ideal.
    ht = ht.filter(ht.alleles[1] != '<DEL>')
    ht = ht.select(
        **{
            'accession': ht.rsid,
            'class': ht.info.CLASS,
        },
    )
    # HGMD duplicates exist due to phenotype information
    # arbitrarily choosing a value should be sufficient.
    return ht.distinct()
