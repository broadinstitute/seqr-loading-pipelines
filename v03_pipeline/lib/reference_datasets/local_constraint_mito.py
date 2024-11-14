import hail as hl

from v03_pipeline.lib.model import ReferenceGenome


def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = hl.import_table(
        raw_dataset_path,
        types={'Position': hl.tint32, 'MLC_score': hl.tfloat32},
    )
    ht = ht.select(
        locus=hl.locus('chrM', ht.Position, reference_genome.value),
        alleles=[ht.Reference, ht.Alternate],
        score=ht.MLC_score,
    )
    return ht.key_by('locus', 'alleles')
