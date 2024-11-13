import hail as hl

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


def filter_contigs(ht, reference_genome: ReferenceGenome):
    return ht.filter(
        hl.set(reference_genome.standard_contigs).contains(ht.locus.contig),
    )
