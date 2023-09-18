from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from v03_pipeline.lib.methods.sex_check import Ploidy

if TYPE_CHECKING:
    import hail as hl


@dataclass
class Lineage:
    mother: str = None
    father: str = None
    maternal_grandmother: str = None
    maternal_grandfather: str = None
    paternal_grandmother: str = None
    paternal_grandfather: str = None
    siblings: list[str] = field(default_factory=list)
    half_siblings: list[str] = field(default_factory=list)
    aunt_uncles: list[str] = field(default_factory=list)


@dataclass
class Family:
    family_guid: str
    sample_lineage: dict[str, Lineage]
    sample_sex: dict[str, Ploidy]

    def __hash__(self):
        return hash(self.family_guid)

    @staticmethod
    def parse_sample_sex(rows: list[hl.Struct]) -> dict[str, Ploidy]:
        sample_sex = {}
        for row in rows:
            sample_sex[row.s] = Ploidy(row.sex)
        return sample_sex

    @staticmethod
    def parse_direct_lineage(rows: list[hl.Struct]) -> dict[str, Lineage]:
        direct_lineage = {}
        for row in rows:
            direct_lineage[row.s] = Lineage(
                mother=row.maternal_s,
                father=row.paternal_s,
            )

        for row in rows:
            # Maternal Parents
            maternal_s = direct_lineage[row.s].mother
            if maternal_s and direct_lineage[maternal_s].mother:
                direct_lineage[row.s].maternal_grandmother = direct_lineage[
                    maternal_s
                ].mother
            if maternal_s and direct_lineage[maternal_s].father:
                direct_lineage[row.s].maternal_grandfather = direct_lineage[
                    maternal_s
                ].father

            # Paternal Parents
            paternal_s = direct_lineage[row.s].father
            if paternal_s and direct_lineage[paternal_s].mother:
                direct_lineage[row.s].paternal_grandmother = direct_lineage[
                    paternal_s
                ].mother
            if paternal_s and direct_lineage[paternal_s].father:
                direct_lineage[row.s].paternal_grandfather = direct_lineage[
                    paternal_s
                ].father
        return direct_lineage

    @staticmethod
    def parse_collateral_lineage(
        sample_lineage: dict[str, Lineage],
    ) -> dict[str, Lineage]:
        for sample_i, sample_j in itertools.combinations(sample_lineage.keys(), 2):
            # If other sample is mother or father, continue
            if (
                sample_j == sample_lineage[sample_i].mother
                or sample_j == sample_lineage[sample_i].father
            ):
                continue

            # If both parents are not None and the same, samples are siblings.
            if (
                sample_lineage[sample_i].mother
                and sample_lineage[sample_i].father
                and (sample_lineage[sample_i].mother == sample_lineage[sample_j].mother)
                and (sample_lineage[sample_i].father == sample_lineage[sample_j].father)
            ):
                sample_lineage[sample_i].siblings.append(
                    sample_j,
                )
                continue

            # If only a single parent is non-null and the same, samples are half siblings
            if (
                sample_lineage[sample_i].mother
                and sample_lineage[sample_i].mother == sample_lineage[sample_j].mother
            ) or (
                sample_lineage[sample_i].father
                and sample_lineage[sample_i].father == sample_lineage[sample_j].father
            ):
                sample_lineage[sample_i].half_siblings.append(
                    sample_j,
                )
                continue

            # If either set of one sample's grandparents is equal the other's parents,
            # they're aunt/uncle
            if (
                sample_lineage[sample_i].maternal_grandmother
                and sample_lineage[sample_i].maternal_grandfather
                and (
                    sample_lineage[sample_i].maternal_grandmother
                    == sample_lineage[sample_j].mother
                )
                and (
                    sample_lineage[sample_i].maternal_grandfather
                    == sample_lineage[sample_j].father
                )
            ) or (
                sample_lineage[sample_i].paternal_grandmother
                and sample_lineage[sample_i].paternal_grandfather
                and (
                    sample_lineage[sample_i].paternal_grandmother
                    == sample_lineage[sample_j].mother
                )
                and (
                    sample_lineage[sample_i].paternal_grandfather
                    == sample_lineage[sample_j].father
                )
            ):
                sample_lineage[sample_i].aunt_uncles.append(
                    sample_j,
                )
        return sample_lineage

    @classmethod
    def parse(cls, family_guid: str, rows: list[hl.Struct]) -> Family:
        sample_lineage = cls.parse_direct_lineage(rows)
        sample_lineage = cls.parse_collateral_lineage(sample_lineage)
        sample_sex = cls.parse_sample_sex(rows)
        return cls(
            family_guid=family_guid,
            sample_lineage=sample_lineage,
            sample_sex=sample_sex,
        )


def parse_pedigree_ht_to_families(
    pedigree_ht: hl.Table,
) -> list[Family]:
    families = []
    for family_guid, rows in itertools.groupby(
        pedigree_ht.collect(),
        lambda x: x.family_guid,
    ):
        families.append(Family.parse(family_guid, sorted(rows, key=lambda x: x.s)))
    return families
