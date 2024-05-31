# ruff: noqa: N806
from typing import Any

import hail as hl

from v03_pipeline.lib.annotations.enums import (
    MOTIF_CONSEQUENCE_TERMS,
    REGULATORY_BIOTYPES,
    REGULATORY_CONSEQUENCE_TERMS,
)
from v03_pipeline.lib.annotations.vep import (
    add_transcript_rank,
    transcript_consequences_sort,
    vep_85_transcript_consequences_select,
    vep_110_transcript_consequences_select,
)
from v03_pipeline.lib.model.definitions import ReferenceGenome

MOTIF_CONSEQUENCE_TERMS_LOOKUP = hl.dict(
    hl.enumerate(MOTIF_CONSEQUENCE_TERMS, index_first=False),
)
REGULATORY_BIOTYPE_LOOKUP = hl.dict(
    hl.enumerate(REGULATORY_BIOTYPES, index_first=False),
)
REGULATORY_CONSEQUENCE_TERMS_LOOKUP = hl.dict(
    hl.enumerate(REGULATORY_CONSEQUENCE_TERMS, index_first=False),
)

N_ALT_REF = 0
N_ALT_HET = 1
N_ALT_HOM = 2


def AB(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.bind(
        lambda total: hl.if_else(
            (is_called) & (total != 0) & (hl.len(mt.AD) > 1),
            hl.float32(mt.AD[1] / total),
            hl.missing(hl.tfloat32),
        ),
        hl.sum(mt.AD),
    )


def DP(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.if_else(
        is_called & hl.is_defined(mt.AD),
        hl.int32(hl.min(hl.sum(mt.AD), 32000)),
        hl.missing(hl.tint32),
    )


def gt_stats(
    ht: hl.Table,
    lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    row = lookup_ht[ht.key]
    ref_samples = hl.sum(hl.flatten(row.project_stats.ref_samples))
    het_samples = hl.sum(hl.flatten(row.project_stats.het_samples))
    hom_samples = hl.sum(hl.flatten(row.project_stats.hom_samples))
    AC = ref_samples * N_ALT_REF + het_samples * N_ALT_HET + hom_samples * N_ALT_HOM
    AN = 2 * (ref_samples + het_samples + hom_samples)
    hom = hom_samples
    return hl.Struct(
        AC=AC,
        AN=AN,
        AF=hl.float32(AC / AN),
        hom=hom,
    )


def gnomad_non_coding_constraint(
    ht: hl.Table,
    interval_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.Struct(
        z_score=(
            interval_ht.index(ht.locus, all_matches=True)
            .filter(
                lambda x: hl.is_defined(x.gnomad_non_coding_constraint['z_score']),
            )
            .gnomad_non_coding_constraint.z_score.first()
        ),
    )


def screen(
    ht: hl.Table,
    interval_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.Struct(
        region_type_ids=(
            interval_ht.index(
                ht.locus,
                all_matches=True,
            ).flatmap(
                lambda x: x.screen['region_type_ids'],
            )
        ),
    )


def check_ref(ht: hl.Table, **_: Any) -> hl.BooleanExpression:
    return hl.is_defined(ht.vep.check_ref)


def sorted_motif_feature_consequences(
    ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    # NB: hl.min() doesn't work correctly in the "sorted" key expression, so we have an additional
    # is_defined check, even though the expression should work on missing on its own.
    # hail bug report incoming.
    result = hl.or_missing(
        hl.is_defined(ht.vep.motif_feature_consequences),
        hl.sorted(
            ht.vep.motif_feature_consequences.map(
                lambda c: c.select(
                    consequence_term_ids=c.consequence_terms.map(
                        lambda t: MOTIF_CONSEQUENCE_TERMS_LOOKUP[t],
                    ),
                    motif_feature_id=c.motif_feature_id,
                ),
            ).filter(lambda c: c.consequence_term_ids.size() > 0),
            lambda c: hl.min(c.consequence_term_ids),
        ),
    )
    return add_transcript_rank(result)


def sorted_regulatory_feature_consequences(
    ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    result = hl.or_missing(
        hl.is_defined(ht.vep.regulatory_feature_consequences),
        hl.sorted(
            ht.vep.regulatory_feature_consequences.map(
                lambda c: c.select(
                    biotype_id=REGULATORY_BIOTYPE_LOOKUP[c.biotype],
                    consequence_term_ids=c.consequence_terms.map(
                        lambda t: REGULATORY_CONSEQUENCE_TERMS_LOOKUP[t],
                    ),
                    regulatory_feature_id=c.regulatory_feature_id,
                ),
            ).filter(lambda c: c.consequence_term_ids.size() > 0),
            lambda c: hl.min(c.consequence_term_ids),
        ),
    )
    return add_transcript_rank(result)


def sorted_transcript_consequences(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    **_: Any,
) -> hl.Expression:
    result = hl.sorted(
        ht.vep.transcript_consequences.map(
            vep_85_transcript_consequences_select
            if reference_genome == ReferenceGenome.GRCh37
            else vep_110_transcript_consequences_select,
        ).filter(lambda c: c.consequence_term_ids.size() > 0),
        transcript_consequences_sort(ht),
    )
    return add_transcript_rank(result)
