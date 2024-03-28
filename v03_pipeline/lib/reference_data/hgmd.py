import hail as hl

from v03_pipeline.lib.model.definitions import ReferenceGenome


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
        contig_recoding=reference_genome.contig_recoding()
    )
    return mt.rows()
