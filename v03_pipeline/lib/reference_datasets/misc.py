import hail as hl

from v03_pipeline.lib.model.definitions import ReferenceGenome


def enum_map(field: hl.Expression, enum_values: list[str]) -> hl.Expression:
    return _enum_map(field, enum_values)[0]


def _enum_map(field: hl.Expression, enum_values: list[str]) -> tuple[hl.Expression, bool]:
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
        ), True
    return lookup[field], False

def get_enum_field_expressions(ht: hl.Table, enum_select: dict) -> dict:
    enum_select_fields = {}
    for field_name, enum_values in enum_select.items():
        expression, is_array = _enum_map(ht[field_name], enum_values)
        enum_select_fields[f"{field_name}_id{'s' if is_array else ''}"] = expression
    return enum_select_fields


def filter_contigs(ht, reference_genome: ReferenceGenome):
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
