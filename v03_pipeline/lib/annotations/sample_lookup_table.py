from typing import Any

import hail as hl

N_ALT_REF = 0
N_ALT_HET = 1
N_ALT_HOM = 2


def AC(  # noqa: N802
    ht: hl.Table,
    sample_lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return (
        sample_lookup_ht[ht.key].ref_samples.length() * N_ALT_REF
        + sample_lookup_ht[ht.key].het_samples.length() * N_ALT_HET
        + sample_lookup_ht[ht.key].hom_samples.length() * N_ALT_HOM
    )


def AN(  # noqa: N802
    ht: hl.Table,
    sample_lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return 2 * (
        sample_lookup_ht[ht.key].ref_samples.length()
        + sample_lookup_ht[ht.key].het_samples.length()
        + sample_lookup_ht[ht.key].hom_samples.length()
    )


def AF(  # noqa: N802
    ht: hl.Table,
    sample_lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return AC(ht, sample_lookup_ht) / AN(ht, sample_lookup_ht)


def hom(
    ht: hl.Table,
    sample_lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return sample_lookup_ht[ht.key].hom_samples.length()
