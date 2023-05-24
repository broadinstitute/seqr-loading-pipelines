import hail as hl

from hail_scripts.utils.hail_utils import import_vcf

PARTITIONS = 100

def download_and_import_hgmd_vcf(
    hgmd_url: str,
    genome_version: str,
) -> hl.Table:
    if genome_version not in ['37', '38']:
        raise ValueError('Invalid genome_version: ' + str(genome_version))
    mt_contig_recoding = {'MT': 'chrM'} if genome_version == '38' else None
    mt = import_vcf(
        hgmd_url,
        genome_version=genome_version,
        force=True,
        min_partitions=PARTITIONS,
        skip_invalid_loci=True,
    )
    return mt.rows()