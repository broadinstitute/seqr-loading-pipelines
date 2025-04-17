import hail as hl

from v03_pipeline.lib.model.definitions import ReferenceGenome


def snake_to_camelcase(snake_string: str):
    components = snake_string.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def camelcase_hl_struct(s: hl.StructExpression) -> hl.StructExpression:
    return s.rename({f: snake_to_camelcase(f) for f in s.keys()})


def camelcase_array_structexpression_fields(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
):
    for field in ht.row:
        if not isinstance(
            ht[field],
            hl.expr.expressions.typed_expressions.ArrayStructExpression,
        ):
            continue
        ht = ht.transmute(
            **{
                snake_to_camelcase(field): ht[field].map(
                    lambda c: camelcase_hl_struct(c),
                ),
            },
        )

    # Custom handling of nested sorted_transcript_consequences fields for GRCh38
    if (
        reference_genome == ReferenceGenome.GRCh38
        and 'sortedTranscriptConsequences' in ht.row
    ):
        ht = ht.annotate(
            sortedTranscriptConsequences=ht.sortedTranscriptConsequences.map(
                lambda s: s.annotate(
                    loftee=camelcase_hl_struct(s.loftee),
                    utrannotator=camelcase_hl_struct(s.utrannotator),
                ),
            ),
        )
    return ht
