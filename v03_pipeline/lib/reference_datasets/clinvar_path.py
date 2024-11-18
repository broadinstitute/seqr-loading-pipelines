import hail as hl

from v03_pipeline.lib.annotations.enums import (
    CLINVAR_PATHOGENICITIES_LOOKUP,
)

CLINVAR_PATH_RANGE = ('Pathogenic', 'Pathogenic/Likely_risk_allele')
CLINVAR_LIKELY_PATH_RANGE = ('Pathogenic/Likely_pathogenic', 'Likely_risk_allele')


def get_ht(
    ht: hl.Table,
    *_,
) -> hl.Table:
    ht = ht.select_globals()
    ht.describe()
    ht = ht.select(
        is_pathogenic=(
            (
                ht.pathogenicity_id
                >= CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_PATH_RANGE[0]]
            )
            & (
                ht.pathogenicity_id
                <= CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_PATH_RANGE[1]]
            )
        ),
        is_likely_pathogenic=(
            (
                ht.pathogenicity_id
                >= CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_LIKELY_PATH_RANGE[0]]
            )
            & (
                ht.pathogenicity_id
                <= CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_LIKELY_PATH_RANGE[1]]
            )
        ),
    )
    return ht.filter(ht.is_pathogenic | ht.is_likely_pathogenic)
