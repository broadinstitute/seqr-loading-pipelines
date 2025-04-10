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
from v03_pipeline.lib.model import DatasetType
from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset


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
                                    lambda idx: ht.enums[annotation_name][enum_name][
                                        idx
                                    ]
                                ),  # noqa: B023
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


def annotate_formatting_annotation_enum_globals(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> hl.Table:
    formatting_annotation_names = {
        fa.__name__ for fa in dataset_type.formatting_annotation_fns(reference_genome)
    }
    if 'sorted_motif_feature_consequences' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sorted_motif_feature_consequences=hl.Struct(
                    consequence_term=MOTIF_CONSEQUENCE_TERMS,
                ),
            ),
        )
    if 'sorted_regulatory_feature_consequences' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sorted_regulatory_feature_consequences=hl.Struct(
                    biotype=REGULATORY_BIOTYPES,
                    consequence_term=REGULATORY_CONSEQUENCE_TERMS,
                ),
            ),
        )
    if 'sorted_transcript_consequences' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sorted_transcript_consequences=hl.Struct(
                    biotype=BIOTYPES,
                    consequence_term=TRANSCRIPT_CONSEQUENCE_TERMS,
                    **(
                        {
                            'loftee': hl.Struct(
                                lof_filter=LOF_FILTERS,
                            ),
                            'utrannotator': hl.Struct(
                                fiveutr_consequence=FIVEUTR_CONSEQUENCES,
                            ),
                        }
                        if reference_genome == ReferenceGenome.GRCh38
                        and dataset_type == DatasetType.SNV_INDEL
                        else {
                            'lof_filter': LOF_FILTERS,
                        }
                    ),
                ),
            ),
        )
    if 'mitotip' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                mitotip=hl.Struct(
                    trna_prediction=MITOTIP_PATHOGENICITIES,
                ),
            ),
        )
    if 'sv_type_id' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sv_type=SV_TYPES,
            ),
        )
    if 'sv_type_detail_id' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(sv_type_detail=SV_TYPE_DETAILS),
        )
    if 'sorted_gene_consequences' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sorted_gene_consequences=hl.Struct(
                    major_consequence=SV_CONSEQUENCE_RANKS,
                ),
            ),
        )
    return ht
