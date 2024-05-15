from typing import Any

import hail as hl

from v03_pipeline.lib.annotations.enums import BIOTYPES, CONSEQUENCE_TERMS, LOF_FILTERS

BIOTYPE_LOOKUP = hl.dict(hl.enumerate(BIOTYPES, index_first=False))
CONSEQUENCE_TERMS_LOOKUP = hl.dict(hl.enumerate(CONSEQUENCE_TERMS, index_first=False))
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

def check_ref(ht: hl.Table, **_: Any) -> hl.BooleanExpression:
    return hl.is_defined(ht.vep.check_ref)

def sorted_transcript_consequences(ht: hl.Table, **_: Any) -> hl.Expression:
    result = hl.sorted(
        ht.vep.transcript_consequences.map(
            lambda c: c.select(
                *SELECTED_ANNOTATIONS,
                biotype_id=BIOTYPE_LOOKUP[c.biotype],
                consequence_term_ids=(
                    c.consequence_terms.filter(
                        lambda t: ~OMIT_CONSEQUENCE_TERMS.contains(t),
                    ).map(lambda t: CONSEQUENCE_TERMS_LOOKUP[t])
                ),
                exon=c.exon.split('/').map(hl.parse_int32),
                is_lof_nagnag=c.lof_flags == 'NAGNAG_SITE',
                intron=c.intron.split('/').map(hl.parse_int32),
                lof_filter_ids=hl.or_missing(
                    (c.lof == 'LC') & hl.is_defined(c.lof_filter),
                    c.lof_filter.split('&|,').map(lambda f: LOF_FILTERS_LOOKUP[f]),
                ),
            ),
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
