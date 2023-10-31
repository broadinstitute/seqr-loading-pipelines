import hail as hl


def download_and_import_hgmd_vcf(
    hgmd_url: str,
    genome_version: str,
) -> hl.Table:
    if genome_version not in ['37', '38']:
        raise ValueError('Invalid genome_version: ' + str(genome_version))
    mt = hl.import_vcf(
        hgmd_url,
        genome_version=f'GRCh{genome_version}',
        force=True,
        min_partitions=100,
        skip_invalid_loci=True,
    )
    return mt.rows()
