from collections.abc import Callable
from typing import Any

import hail as hl

from v03_pipeline.lib.annotations.enums import (
    BIOTYPES,
    FIVEUTR_CONSEQUENCES,
    LOF_FILTERS,
    MOTIF_CONSEQUENCE_TERMS,
    REGULATORY_CONSEQUENCE_TERMS,
    TRANSCRIPT_CONSEQUENCE_TERMS,
)
from v03_pipeline.lib.model.definitions import ReferenceGenome

BIOTYPE_LOOKUP = hl.dict(hl.enumerate(BIOTYPES, index_first=False))
EXTENDED_INTRONIC_SPLICE_REGION_VARIANT = 'extended_intronic_splice_region_variant'
FIVEUTR_CONSEQUENCES_LOOKUP = hl.dict(
    hl.enumerate(FIVEUTR_CONSEQUENCES, index_first=False).extend(
        [(hl.missing(hl.tstr), hl.missing(hl.tint32))],
    ),
)
LOF_FILTERS_LOOKUP = hl.dict(hl.enumerate(LOF_FILTERS, index_first=False))
MANE_SELECT_ANNOTATIONS = [
    'mane_select',
    'mane_plus_clinical',
]
MOTIF_CONSEQUENCE_TERMS_LOOKUP = hl.dict(
    hl.enumerate(MOTIF_CONSEQUENCE_TERMS, index_first=False),
)
NAGNAG_SITE = 'NAGNAG_SITE'
OMIT_TRANSCRIPT_CONSEQUENCE_TERMS = hl.set(
    [
        'upstream_gene_variant',
        'downstream_gene_variant',
    ],
)
PROTEIN_CODING_ID = BIOTYPE_LOOKUP['protein_coding']
REGULATORY_CONSEQUENCE_TERMS_LOOKUP = hl.dict(
    hl.enumerate(REGULATORY_CONSEQUENCE_TERMS, index_first=False),
)
SELECTED_ANNOTATIONS = [
    'amino_acids',
    'canonical',
    'codons',
    'gene_id',
    'hgvsc',
    'hgvsp',
    'transcript_id',
]
TRANSCRIPT_CONSEQUENCE_TERMS_LOOKUP = hl.dict(
    hl.enumerate(TRANSCRIPT_CONSEQUENCE_TERMS, index_first=False),
)


def _add_transcript_rank(result: hl.ArrayExpression) -> hl.ArrayExpression:
    # Adds a "transcript_rank" field to a sorted array of transcripts
    return hl.zip_with_index(result).map(
        lambda csq_with_index: csq_with_index[1].annotate(
            transcript_rank=csq_with_index[0],
        ),
    )


def _lof_filter_ids(c: hl.StructExpression) -> hl.ArrayNumericExpression:
    return hl.or_missing(
        (c.lof == 'LC') & hl.is_defined(c.lof_filter),
        c.lof_filter.split('&|,').map(lambda f: LOF_FILTERS_LOOKUP[f]),
    )


def _consequence_term_ids(c: hl.StructExpression) -> hl.ArrayNumericExpression:
    return c.consequence_terms.filter(
        lambda t: ~OMIT_TRANSCRIPT_CONSEQUENCE_TERMS.contains(t),
    ).map(lambda t: TRANSCRIPT_CONSEQUENCE_TERMS_LOOKUP[t])


def _transcript_consequences_select(
    reference_genome: ReferenceGenome,
) -> Callable[[hl.StructExpression], hl.StructExpression]:
    if reference_genome == ReferenceGenome.GRCh38:
        return lambda c: c.select(
            *SELECTED_ANNOTATIONS,
            *MANE_SELECT_ANNOTATIONS,
            biotype_id=BIOTYPE_LOOKUP[c.biotype],
            consequence_term_ids=_consequence_term_ids(c),
            exon=hl.bind(
                lambda split: hl.or_missing(
                    hl.is_defined(split),
                    hl.Struct(index=split[0], total=split[1]),
                ),
                c.exon.split('/').map(hl.parse_int32),
            ),
            intron=hl.bind(
                lambda split: hl.or_missing(
                    hl.is_defined(split),
                    hl.Struct(index=split[0], total=split[1]),
                ),
                c.intron.split('/').map(hl.parse_int32),
            ),
            alphamissense=hl.struct(
                pathogenicity=c.am_pathogenicity,
            ),
            loftee=hl.struct(
                is_lof_nagnag=c.lof_flags == NAGNAG_SITE,
                lof_filter_ids=_lof_filter_ids(c),
            ),
            spliceregion=hl.struct(
                extended_intronic_splice_region_variant=(
                    hl.is_defined(c.spliceregion)
                    & c.spliceregion.contains(
                        EXTENDED_INTRONIC_SPLICE_REGION_VARIANT,
                    )
                ),
            ),
            utrrannotator=hl.struct(
                existing_inframe_oorfs=c.existing_inframe_oorfs,
                existing_outofframe_oorfs=c.existing_outofframe_oorfs,
                existing_uorfs=c.existing_uorfs,
                fiveutr_consequence_id=FIVEUTR_CONSEQUENCES_LOOKUP[
                    c.fiveutr_consequence
                ],
                # Annotation documentation here:
                # https://github.com/ImperialCardioGenetics/UTRannotator?tab=readme-ov-file#the-detailed-annotation-for-each-consequence
                # NB: 
                fiveutr_annotation=c.fiveutr_annotation['1'].annotate(
                    AltStopDistanceToCDS=hl.parse_int32(c.fiveutr_annotation['1'].AltStopDistanceToCDS),
                    CapDistanceToStart=hl.parse_int32(c.fiveutr_annotation['1'].CapDistanceToStart),
                    DistanceToCDS=hl.parse_int32(c.fiveutr_annotation['1'].DistanceToCDS),
                    DistanceToStop=hl.parse_int32(c.fiveutr_annotation['1'].DistanceToStop),
                    Evidence=hl.or_missing(
                        # Just in case a weird value ("NA" or anything else) propagates
                        (
                            (c.fiveutr_annotation['1'].Evidence == 'True') | (c.fiveutr_annotation['1'].Evidence == 'False')
                        ),
                        hl.bool(c.fiveutr_annotation['1'].Evidence),
                    ),
                    StartDistanceToCDS=hl.parse_int32(c.fiveutr_annotation['1'].StartDistanceToCDS),
                    newSTOPDistanceToCDS=hl.parse_int32(c.fiveutr_annotation['1'].newSTOPDistanceToCDS),
                    alt_type_length=hl.parse_int32(c.fiveutr_annotation['1'].alt_type_length),
                    ref_StartDistanceToCDS=hl.parse_int32(c.fiveutr_annotation['1'].ref_StartDistanceToCDS),
                    ref_type_length=hl.parse_int32(c.fiveutr_annotation['1'].ref_type_length),
                ),
            ),
        )
    return lambda c: c.select(
        *SELECTED_ANNOTATIONS,
        biotype_id=BIOTYPE_LOOKUP[c.biotype],
        consequence_term_ids=_consequence_term_ids(c),
        is_lof_nagnag=c.lof_flags == NAGNAG_SITE,
        lof_filter_ids=_lof_filter_ids(c),
    )


def check_ref(ht: hl.Table, **_: Any) -> hl.BooleanExpression:
    return hl.is_defined(ht.vep.check_ref)


def sorted_motif_feature_consequences(
    ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    result = hl.sorted(
        ht.vep.motif_feature_consequences.map(
            lambda c: c.select(
                consequence_term_ids=c.consequence_terms.map(
                    lambda t: MOTIF_CONSEQUENCE_TERMS_LOOKUP[t],
                ),
                motif_feature_id=c.motif_feature_id,
            ).filter(lambda c: c.consequence_term_ids.size() > 0),
            lambda c: hl.min(c.consequence_term_ids),
        ),
    )
    return _add_transcript_rank(result)


def sorted_regulatory_feature_consequences(
    ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    result = hl.sorted(
        ht.vep.regulatory_feature_consequences.map(
            lambda c: c.select(
                biotype_id=BIOTYPE_LOOKUP[c.biotype],
                consequence_term_ids=c.consequence_terms.map(
                    lambda t: REGULATORY_CONSEQUENCE_TERMS_LOOKUP[t],
                ),
                regulatory_feature_id=c.regulatory_feature_id,
            ).filter(lambda c: c.consequence_term_ids.size() > 0),
            lambda c: hl.min(c.consequence_term_ids),
        ),
    )
    return _add_transcript_rank(result)


def sorted_transcript_consequences(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    **_: Any,
) -> hl.Expression:
    result = hl.sorted(
        ht.vep.transcript_consequences.map(
            _transcript_consequences_select(reference_genome),
        ).filter(lambda c: c.consequence_term_ids.size() > 0),
        lambda c: (
            hl.bind(
                lambda is_coding, is_most_severe, is_canonical: (
                    hl.cond(
                        is_coding,
                        hl.cond(
                            is_most_severe,
                            hl.cond(is_canonical, 1, 2),
                            hl.cond(is_canonical, 3, 4),
                        ),
                        hl.cond(
                            is_most_severe,
                            hl.cond(is_canonical, 5, 6),
                            hl.cond(is_canonical, 7, 8),
                        ),
                    )
                ),
                c.biotype_id == PROTEIN_CODING_ID,
                hl.set(c.consequence_term_ids).contains(
                    TRANSCRIPT_CONSEQUENCE_TERMS_LOOKUP[ht.vep.most_severe_consequence],
                ),
                hl.or_else(c.canonical, 0) == 1,
            )
        ),
    )
    return _add_transcript_rank(result)
