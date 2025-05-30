import hail as hl

from v03_pipeline.lib.annotations.enums import (
    BIOTYPES,
    FIVEUTR_CONSEQUENCES,
    LOF_FILTERS,
    MITOTIP_PATHOGENICITIES,
    MOTIF_CONSEQUENCE_TERMS,
    REGULATORY_BIOTYPES,
    REGULATORY_CONSEQUENCE_TERMS,
    SV_CONSEQUENCE_RANKS,
    SV_TYPE_DETAILS,
    SV_TYPES,
    TRANSCRIPT_CONSEQUENCE_TERMS,
)
from v03_pipeline.lib.misc.nested_field import parse_nested_field
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset


def snake_to_camelcase(snake_string: str):
    components = snake_string.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def camelcase_hl_struct(s: hl.StructExpression) -> hl.StructExpression:
    return s.rename({f: snake_to_camelcase(f) for f in s})


def sorted_hl_struct(s: hl.StructExpression) -> hl.StructExpression:
    if not isinstance(s, hl.StructExpression):
        return s
    return s.select(**{k: sorted_hl_struct(s[k]) for k in sorted(s)})


def array_structexpression_fields(ht: hl.Table):
    return [
        field
        for field in ht.row
        if isinstance(
            ht[field],
            hl.expr.expressions.typed_expressions.ArrayStructExpression,
        )
    ]


def transcripts_field_name(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    formatting_annotation_names = {
        fa.__name__ for fa in dataset_type.formatting_annotation_fns(reference_genome)
    }
    if 'sorted_gene_consequences' in formatting_annotation_names:
        return snake_to_camelcase('sorted_gene_consequences')
    return snake_to_camelcase('sorted_transcript_consequences')


def subset_filterable_transcripts_fields(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> hl.Table:
    field_name = transcripts_field_name(reference_genome, dataset_type)
    return ht.annotate(
        **{
            field_name: hl.enumerate(ht[field_name]).starmap(
                lambda idx, c: c.select(
                    **{
                        new_nested_field_name: parse_nested_field(
                            ht[field_name],
                            existing_nested_field_name,
                        )[idx]
                        for new_nested_field_name, existing_nested_field_name in dataset_type.export_parquet_filterable_transcripts_fields(
                            reference_genome,
                        ).items()
                    },
                ),
            ),
        },
    )


def camelcase_array_structexpression_fields(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
):
    for field in array_structexpression_fields(ht):
        ht = ht.transmute(
            **{
                snake_to_camelcase(field): ht[field].map(
                    lambda c: camelcase_hl_struct(c),
                ),
            },
        )

    # Custom handling of nested sorted_transcript_consequences fields for GRCh38/SNV_INDEL.
    # Note that spliceregion (extended_intronic_splice_region_variant) prevents
    # a more procedural approach here.
    if (
        reference_genome == ReferenceGenome.GRCh38
        and dataset_type == DatasetType.SNV_INDEL
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


def unmap_reference_dataset_annotation_enums(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> hl.Table:
    reference_datasets = ReferenceDataset.for_reference_genome_dataset_type_annotations(
        reference_genome,
        dataset_type,
    )
    unmapped_annotation_name = []
    for annotation_name in ht.enums:
        if annotation_name not in reference_datasets:
            continue
        for enum_name in ht.enums[annotation_name]:
            if hasattr(ht[annotation_name], f'{enum_name}_ids'):
                ht = ht.annotate(
                    **{
                        annotation_name: ht[annotation_name].annotate(
                            **{
                                f'{enum_name}s': ht[annotation_name][
                                    f'{enum_name}_ids'
                                ].map(
                                    lambda idx: ht.enums[annotation_name][enum_name][  # noqa: B023
                                        idx
                                    ],
                                ),
                            },
                        ),
                    },
                )
                ht = ht.annotate(
                    **{annotation_name: ht[annotation_name].drop(f'{enum_name}_ids')},
                )
            else:
                ht = ht.annotate(
                    **{
                        annotation_name: ht[annotation_name].annotate(
                            **{
                                enum_name: ht.enums[annotation_name][enum_name][
                                    ht[annotation_name][f'{enum_name}_id']
                                ],
                            },
                        ),
                    },
                )
                ht = ht.annotate(
                    **{annotation_name: ht[annotation_name].drop(f'{enum_name}_id')},
                )
        unmapped_annotation_name.append(annotation_name)

    # Explicit clinvar edge case:
    if hasattr(ht, ReferenceDataset.clinvar.value):
        ht = ht.annotate(
            **{
                ReferenceDataset.clinvar.value: ht[
                    ReferenceDataset.clinvar.value
                ].annotate(
                    conflictingPathogenicities=ht[
                        ReferenceDataset.clinvar.value
                    ].conflictingPathogenicities.map(
                        lambda s: s.annotate(
                            pathogenicity=ht.enums.clinvar.pathogenicity[
                                s.pathogenicity_id
                            ],
                        ).drop('pathogenicity_id'),
                    ),
                ),
            },
        )

    # Explicit hgmd edge case:
    if hasattr(
        ht,
        ReferenceDataset.hgmd.value,
    ):
        ht = ht.annotate(
            **{
                ReferenceDataset.hgmd.value: ht[ReferenceDataset.hgmd.value]
                .annotate(classification=ht[ReferenceDataset.hgmd.value]['class'])
                .drop('class'),
            },
        )
    return ht.annotate_globals(enums=ht.globals.enums.drop(*unmapped_annotation_name))


def unmap_formatting_annotation_enums(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> hl.Table:
    formatting_annotation_names = {
        fa.__name__ for fa in dataset_type.formatting_annotation_fns(reference_genome)
    }
    if 'sorted_motif_feature_consequences' in formatting_annotation_names:
        ht = ht.annotate(
            sorted_motif_feature_consequences=ht.sorted_motif_feature_consequences.map(
                lambda c: c.annotate(
                    consequence_terms=c.consequence_term_ids.map(
                        lambda tid: hl.array(MOTIF_CONSEQUENCE_TERMS)[tid],
                    ),
                ).drop('consequence_term_ids'),
            ),
        )
        ht = ht.annotate_globals(
            enums=ht.enums.drop('sorted_motif_feature_consequences'),
        )
    if 'sorted_regulatory_feature_consequences' in formatting_annotation_names:
        ht = ht.annotate(
            sorted_regulatory_feature_consequences=ht.sorted_regulatory_feature_consequences.map(
                lambda c: c.annotate(
                    biotype=hl.array(REGULATORY_BIOTYPES)[c.biotype_id],
                    consequence_terms=c.consequence_term_ids.map(
                        lambda tid: hl.array(REGULATORY_CONSEQUENCE_TERMS)[tid],
                    ),
                ).drop('biotype_id', 'consequence_term_ids'),
            ),
        )
        ht = ht.annotate_globals(
            enums=ht.enums.drop('sorted_regulatory_feature_consequences'),
        )
    if 'sorted_transcript_consequences' in formatting_annotation_names:
        ht = ht.annotate(
            sorted_transcript_consequences=ht.sorted_transcript_consequences.map(
                lambda c: c.annotate(
                    biotype=hl.array(BIOTYPES)[c.biotype_id],
                    consequence_terms=c.consequence_term_ids.map(
                        lambda tid: hl.array(TRANSCRIPT_CONSEQUENCE_TERMS)[tid],
                    ),
                    **{
                        'loftee': c.loftee.annotate(
                            lof_filters=c.loftee.lof_filter_ids.map(
                                lambda fid: hl.array(LOF_FILTERS)[fid],
                            ),
                        ).drop('lof_filter_ids'),
                        'utrannotator': c.utrannotator.annotate(
                            fiveutr_consequence=hl.array(FIVEUTR_CONSEQUENCES)[
                                c.utrannotator.fiveutr_consequence_id
                            ],
                        ).drop('fiveutr_consequence_id'),
                    }
                    if reference_genome == ReferenceGenome.GRCh38
                    and dataset_type == DatasetType.SNV_INDEL
                    else {
                        'lof_filters': c.lof_filter_ids.map(
                            lambda fid: hl.array(LOF_FILTERS)[fid],
                        ),
                    },
                ).drop(
                    'biotype_id',
                    'consequence_term_ids',
                    *(
                        []
                        if reference_genome == ReferenceGenome.GRCh38
                        and dataset_type == DatasetType.SNV_INDEL
                        else [
                            'lof_filter_ids',
                        ]
                    ),
                ),
            ),
        )
        ht = ht.annotate_globals(enums=ht.enums.drop('sorted_transcript_consequences'))
    if 'mitotip' in formatting_annotation_names:
        ht = ht.annotate(
            mitotip=hl.Struct(
                trna_prediction=hl.array(MITOTIP_PATHOGENICITIES)[
                    ht.mitotip.trna_prediction_id
                ],
            ),
        )
        ht = ht.annotate_globals(enums=ht.enums.drop('mitotip'))
    if 'sv_type_id' in formatting_annotation_names:
        ht = ht.annotate(sv_type=hl.array(SV_TYPES)[ht.sv_type_id]).drop('sv_type_id')
        ht = ht.annotate_globals(enums=ht.enums.drop('sv_type'))
    if 'sv_type_detail_id' in formatting_annotation_names:
        ht = ht.annotate(
            sv_type_detail=hl.array(SV_TYPE_DETAILS)[ht.sv_type_detail_id],
        ).drop('sv_type_detail_id')
        ht = ht.annotate_globals(enums=ht.enums.drop('sv_type_detail'))
    if 'sorted_gene_consequences' in formatting_annotation_names:
        ht = ht.annotate(
            sorted_gene_consequences=ht.sorted_gene_consequences.map(
                lambda c: c.annotate(
                    major_consequence=hl.array(SV_CONSEQUENCE_RANKS)[
                        c.major_consequence_id
                    ],
                ).drop('major_consequence_id'),
            ),
        )
        ht = ht.annotate_globals(enums=ht.enums.drop('sorted_gene_consequences'))
    return ht
