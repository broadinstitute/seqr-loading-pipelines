from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    import hail as hl


def clinvar_path_and_likely_path(ht: hl.Table) -> hl.Table:
    return ht


def gnomad_coding_and_noncoding(ht: hl.Table) -> hl.Table:
    return ht


def gnomad_high_af(ht: hl.Table) -> hl.Table:
    return ht


class CachedReferenceDatasetQuery(Enum):
    CLINVAR_PATH_VARIANTS = 'clinvar_path_variants'
    GNOMAD_CODING_AND_NONCODING_VARIANTS = 'gnomad_coding_and_noncoding_variants'
    GNOMAD_HIGH_AF_VARIANTS = 'gnomad_high_af_variants'

    @property
    def dataset(self) -> str:
        return {
            CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS: 'clinvar',
            CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS: 'gnomad_genomes',
            CachedReferenceDatasetQuery.GNOMAD_HIGH_AF_VARIANTS: 'gnomad_genomes',
        }[self]

    @property
    def query(self) -> Callable[[hl.Table], hl.Table]:
        return {
            CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS: clinvar_path_and_likely_path,
            CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS: gnomad_coding_and_noncoding,
            CachedReferenceDatasetQuery.GNOMAD_HIGH_AF_VARIANTS: gnomad_high_af,
        }[self]
