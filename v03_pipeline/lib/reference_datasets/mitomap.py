import hail as hl

from v03_pipeline.lib.model import ReferenceGenome


# adapted from download_and_create_reference_datasets/v02/mito/write_mito_mitomap_ht.py
def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = hl.import_table(
        raw_dataset_path,
        delimiter=',',
        types={'"Position"': hl.tstr, '"Allele"': hl.tstr},
    )
    # NB: all strings in the downloaded CSV file are wrapped in double quotes
    ht = ht.rename(
        {
            '"Position"': 'Position',
            '"Allele"': 'Allele',
        },
    )
    ht = ht.select(
        locus=hl.locus(
            'chrM',
            hl.parse_int32(hl.parse_json(ht.Position, dtype='str')),
            reference_genome=reference_genome.value,
        ),
        alleles=ht.Allele.first_match_in('m.[0-9]+([ATGC]+)>([ATGC]+)'),
        pathogenic=True,
    )
    return ht.key_by('locus', 'alleles')
