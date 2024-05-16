from collections.abc import Callable
from typing import Any

import hail as hl

from v03_pipeline.lib.annotations.enums import (
    ALPHAMISSENSE_CLASSES,
    BIOTYPES,
    CONSEQUENCE_TERMS,
    FIVEUTR_CONSEQUENCES,
    LOF_FILTERS,
)
from v03_pipeline.lib.model.definitions import ReferenceGenome

ALPHAMISSENSE_CLASSES_LOOKUP = hl.dict(
    hl.enumerate(ALPHAMISSENSE_CLASSES, index_first=False),
)
BIOTYPE_LOOKUP = hl.dict(hl.enumerate(BIOTYPES, index_first=False))
CONSEQUENCE_TERMS_LOOKUP = hl.dict(hl.enumerate(CONSEQUENCE_TERMS, index_first=False))
FIVEUTR_CONSEQUENCES_LOOKUP = hl.dict(
    hl.enumerate(FIVEUTR_CONSEQUENCES, index_first=False),
)
LOF_FILTERS_LOOKUP = hl.dict(hl.enumerate(LOF_FILTERS, index_first=False))

PROTEIN_CODING_ID = BIOTYPE_LOOKUP['protein_coding']

OMIT_CONSEQUENCE_TERMS = hl.set(
    [
        'upstream_gene_variant',
        'downstream_gene_variant',
    ],
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

MANE_SELECT_ANNOTATIONS = [
    'mane_select',
    'mane_plus_clinical',
]

NAGNAG_SITE = 'NAGNAG_SITE'


def _lof_filter_ids(c: hl.StructExpression) -> hl.ArrayNumericExpression:
    return hl.or_missing(
        (c.lof == 'LC') & hl.is_defined(c.lof_filter),
        c.lof_filter.split('&|,').map(lambda f: LOF_FILTERS_LOOKUP[f]),
    )


def _consequence_term_ids(c: hl.StructExpression) -> hl.ArrayNumericExpression:
    return c.consequence_terms.filter(
        lambda t: ~OMIT_CONSEQUENCE_TERMS.contains(t),
    ).map(lambda t: CONSEQUENCE_TERMS_LOOKUP[t])


def _transcript_consequences_select(
    reference_genome: ReferenceGenome,
) -> Callable[[hl.StructExpression], hl.StructExpression]:
    if reference_genome == ReferenceGenome.GRCh38:
        return lambda c: c.select(
            *SELECTED_ANNOTATIONS,
            *MANE_SELECT_ANNOTATIONS,
            biotype_id=BIOTYPE_LOOKUP[c.biotype],
            consequence_term_ids=_consequence_term_ids(c),
            exon=c.exon.split('/').map(hl.parse_int32),
            intron=c.intron.split('/').map(hl.parse_int32),
            # Plugins
            alphamissense=hl.struct(
                class_id=ALPHAMISSENSE_CLASSES_LOOKUP[c.am_class],
                pathogenicity=c.am_pathogenicity,
            ),
            loftee=hl.struct(
                is_lof_nagnag=c.lof_flags == NAGNAG_SITE,
                lof_filter_ids=_lof_filter_ids(c),
            ),
            # utrannotator annotations
            utrrannotator=hl.struct(
                existing_inframe_oorfs=c.existing_inframe_oorfs,
                existing_outofframe_oorfs=c.existing_outofframe_oorfs,
                existing_uorfs=c.existing_uorfs,
                fiveutr_consequence_id=FIVEUTR_CONSEQUENCES_LOOKUP[
                    c.fiveutr_consequence
                ],
                # Annotation documentation here:
                # https://github.com/ImperialCardioGenetics/UTRannotator?tab=readme-ov-file#the-detailed-annotation-for-each-consequence
                fiveutr_annotation=c.fiveutr_annotation,
            ),
        )
    print('ben')
    return lambda c: c.select(
        *SELECTED_ANNOTATIONS,
        #biotype_id=BIOTYPE_LOOKUP[c.biotype],
        #consequence_term_ids=_consequence_term_ids(c),
        #is_lof_nagnag=c.lof_flags == NAGNAG_SITE,
        #lof_filter_ids=_lof_filter_ids(c),
    )


def check_ref(ht: hl.Table, **_: Any) -> hl.BooleanExpression:
    return hl.is_defined(ht.vep.check_ref)


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
                    CONSEQUENCE_TERMS_LOOKUP[ht.vep.most_severe_consequence],
                ),
                hl.or_else(c.canonical, 0) == 1,
            )
        ),
    )
    return hl.zip_with_index(result).map(
        lambda csq_with_index: csq_with_index[1].annotate(
            transcript_rank=csq_with_index[0],
        ),
    )
