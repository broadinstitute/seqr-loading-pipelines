import hail as hl

from v03_pipeline.lib.model import ReferenceGenome

HGMD_ENUM_SELECT = {
    'class': [
        'DM',
        'DM?',
        'DP',
        'DFP',
        'FP',
        'R',
    ],
}


def download_and_import_hgmd_vcf(
    hgmd_url: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    mt = hl.import_vcf(
        hgmd_url,
        reference_genome=reference_genome.value,
        force=True,
        min_partitions=100,
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
