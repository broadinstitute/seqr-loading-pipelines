import hail as hl

from v03_pipeline.lib.model import ReferenceGenome


# adapted from download_and_create_reference_datasets/v02/mito/write_mito_mitomap_ht.py
def get_ht(path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = hl.import_table(
        path,
        delimiter=',',
        quote='"',
        types={'Position': hl.tint32},
    )
    ht = ht.select(
        locus=hl.locus(
            'chrM',
            ht.Position,
            reference_genome=reference_genome.value,
        ),
        alleles=ht.Allele.first_match_in('m.[0-9]+([ATGC]+)>([ATGC]+)'),
        pathogenic=True,
    )
    return ht.key_by('locus', 'alleles')
