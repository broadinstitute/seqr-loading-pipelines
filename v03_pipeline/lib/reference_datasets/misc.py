import hail as hl

from v03_pipeline.lib.model.dataset_type import DatasetType
from v03_pipeline.lib.model.definitions import ReferenceGenome


def enum_map(field: hl.Expression, enum_values: list[str]) -> dict:
    lookup = hl.dict(
        hl.enumerate(enum_values, index_first=False).extend(
            # NB: adding missing values here allows us to
            # hard fail if a mapped key is present and has an unexpected value
            # but propagate missing values.
            [(hl.missing(hl.tstr), hl.missing(hl.tint32))],
        ),
    )
    if (
        isinstance(field.dtype, hl.tarray | hl.tset)
        and field.dtype.element_type == hl.tstr
    ):
        return field.map(
            lambda x: lookup[x],
        )
    return lookup[field]


def filter_contigs(ht, reference_genome: ReferenceGenome, dataset_type: DatasetType):
    if dataset_type == DatasetType.MITO:
        return ht.filter(ht.locus.contig == reference_genome.mito_contig)
    return ht.filter(
        hl.set(reference_genome.standard_contigs).contains(ht.locus.contig),
    )


def key_by_locus_alleles(ht: hl.Table, reference_genome: ReferenceGenome) -> hl.Table:
    chrom = (
        hl.format('chr%s', ht.chrom)
        if reference_genome == ReferenceGenome.GRCh38
        else ht.chrom
    )
    locus = hl.locus(chrom, ht.pos, reference_genome.value)
    alleles = hl.array([ht.ref, ht.alt])
    ht = ht.transmute(locus=locus, alleles=alleles)
    return ht.key_by('locus', 'alleles')
