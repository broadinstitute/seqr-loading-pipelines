import hail as hl

from v03_pipeline.lib.model import ReferenceGenome


def get_ht(path: str, reference_genome: ReferenceGenome) -> hl.Table:
    mt = hl.import_vcf(
        path,
        reference_genome=reference_genome.value,
        force=True,
        skip_invalid_loci=True,
        contig_recoding=reference_genome.contig_recoding(),
    )
    ht = mt.rows()
    return ht.select(
        **{
            'accession': ht.rsid,
            'class': ht.info.CLASS,
        },
    )
