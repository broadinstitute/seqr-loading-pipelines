from typing import Any

import hail as hl

N_ALT_REF = 0
N_ALT_HET = 1
N_ALT_HOM = 2


def _AC(  # noqa: N802
    ht: hl.Table,
    sample_lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.sum(
        sample_lookup_ht.index_globals().project_guids.map(
            lambda project_guid: (
                sample_lookup_ht[ht.key].ref_samples[project_guid].length() * N_ALT_REF
                + sample_lookup_ht[ht.key].het_samples[project_guid].length()
                * N_ALT_HET
                + sample_lookup_ht[ht.key].hom_samples[project_guid].length()
                * N_ALT_HOM
            ),
        ),
    )


def _AN(  # noqa: N802
    ht: hl.Table,
    sample_lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return 2 * hl.sum(
        sample_lookup_ht.index_globals().project_guids.map(
            lambda project_guid: (
                sample_lookup_ht[ht.key].ref_samples[project_guid].length()
                + sample_lookup_ht[ht.key].het_samples[project_guid].length()
                + sample_lookup_ht[ht.key].hom_samples[project_guid].length()
            ),
        ),
    )


def _AF(  # noqa: N802
    ht: hl.Table,
    sample_lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.float32(_AC(ht, sample_lookup_ht) / _AN(ht, sample_lookup_ht))


def _hom(
    ht: hl.Table,
    sample_lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.sum(
        sample_lookup_ht.index_globals().project_guids.map(
            lambda project_guid: (
                sample_lookup_ht[ht.key].hom_samples[project_guid].length()
            ),
        )
    )


def gt_stats(
    ht: hl.Table,
    sample_lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.Struct(
        AC=_AC(ht, sample_lookup_ht),
        AN=_AN(ht, sample_lookup_ht),
        AF=_AF(ht, sample_lookup_ht),
        hom=_hom(ht, sample_lookup_ht),
    )
