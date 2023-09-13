from __future__ import annotations

from enum import Enum

import hail as hl

IMPUTE_SEX_ANNOTATIONS = [
    'is_female',
    'f_stat',
    'n_called',
    'expected_homs',
    'observed_homs',
    'sex',
]

AMBIGUOUS_THRESHOLD_PERC: float = 0.005  # Fraction of samples identified as "ambiguous_sex" above which an error will be thrown.
AAF_THRESHOLD: float = 0.05  # Alternate allele frequency threshold for `hl.impute_sex`.
XX_FSTAT_THRESHOLD: float = (
    0.5  # F-stat threshold below which a sample will be called XX
)
XY_FSTAT_THRESHOLD: float = (
    0.75  # F-stat threshold above which a sample will be called XY.
)


class Ploidy(Enum):
    AMBIGUOUS = 'ambiguous_sex'
    FEMALE = 'XX'
    MALE = 'XY'


def call_sex(mt: hl.MatrixTable) -> hl.Table:
    # Filter to SNVs and biallelics
    # NB: We should already have filtered biallelics, but just in case.
    mt = mt.filter_rows(hl.is_snp(mt.alleles[0], mt.alleles[1]))

    # Filter to PASS variants only (variants with empty or missing filter set)
    mt = mt.filter_rows(
        hl.is_missing(mt.filters) | (mt.filters.length() == 0),
        keep=True,
    )
    impute_sex_ht = hl.impute_sex(
        mt.GT,
        male_threshold=XY_FSTAT_THRESHOLD,
        female_threshold=XX_FSTAT_THRESHOLD,
        aaf_threshold=AAF_THRESHOLD,
    )
    ht = mt.annotate_cols(**impute_sex_ht[mt.col_key]).cols()
    ht = ht.annotate(
        sex=(
            hl.case()
            .when(hl.is_missing(ht.is_female), Ploidy.AMBIGUOUS.value)
            .when(ht.is_female, Ploidy.FEMALE.value)
            .default(Ploidy.MALE.value)
        ),
    )
    ambiguous_perc = ht.aggregate(
        hl.agg.fraction(ht.sex == Ploidy.AMBIGUOUS.value),
    )
    if ambiguous_perc > AMBIGUOUS_THRESHOLD_PERC:
        msg = f'{ambiguous_perc:.2%} of samples identified as ambiguous.  Please contact the methods team to investigate the callset.'
        raise ValueError(msg)
    return ht.select(*IMPUTE_SEX_ANNOTATIONS)
