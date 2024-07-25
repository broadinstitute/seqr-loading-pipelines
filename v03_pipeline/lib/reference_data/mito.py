import hail as hl

from v03_pipeline.lib.model.definitions import ReferenceGenome


def download_and_import_local_constraint_tsv(
    url: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    ht = hl.import_table(url, types={'Position': hl.tint32, 'MLC_score': hl.tfloat32})
    ht = ht.select(
        locus=hl.locus('chrM', ht.Position, reference_genome.value),
        alleles=[ht.Reference, ht.Alternate],
        MLC_score=ht.MLC_score,
    )
    return ht.key_by('locus', 'alleles')
