from __future__ import annotations

import itertools
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import hail as hl


class DirectRelation(IntEnum):
    MOTHER = 0
    FATHER = 1
    MATERNAL_GRANDMOTHER = 2
    MATERNAL_GRANDFATHER = 3
    PATERNAL_GRANDMOTHER = 4
    PATERNAL_GRANDFATHER = 5

    def coefficient(self):
        if self <= DirectRelation.FATHER:
            return 0.5
        return 0.25


class CollateralRelation(IntEnum):
    SIBLING = 0
    HALF_SIBLING = 1
    AUNT_UNCLE = 2


@dataclass
class Family:
    family_guid: str
    collateral_lineage: dict[str, list[list[str]]]
    direct_lineage: dict[str, list[str]]

    @staticmethod
    def parse_direct_lineage(rows: list[hl.Struct]) -> dict[str, list[str]]:
        direct_lineage = {}
        for row in rows:
            direct_lineage[row.s] = [len(DirectRelation) * None]
            direct_lineage[row.s][DirectRelation.MOTHER] = row.maternal_s
            direct_lineage[row.s][DirectRelation.FATHER] = row.paternal_s

        for row in rows:
            mother = direct_lineage[row.s][DirectRelation.MOTHER]
            if mother:
                direct_lineage[row.s][
                    DirectRelation.MATERNAL_GRANDMOTHER
                ] = direct_lineage[mother.s][DirectRelation.MOTHER]
                direct_lineage[row.s][
                    DirectRelation.MATERNAL_GRANDFATHER
                ] = direct_lineage[mother.s][DirectRelation.FATHER]
            father = direct_lineage[row.s][DirectRelation.FATHER]
            if father:
                direct_lineage[row.s][
                    DirectRelation.PATERNAL_GRANDMOTHER
                ] = direct_lineage[father.s][DirectRelation.MOTHER]
                direct_lineage[row.s][
                    DirectRelation.PATERNAL_GRANDFATHER
                ] = direct_lineage[father.s][DirectRelation.FATHER]
        return direct_lineage

    @staticmethod
    def parse_collateral_lineage(
        direct_lineage: dict[str, list[str]],
    ) -> dict[str, list[list[str]]]:
        collateral_lineage = {}
        for sample_i, sample_j in itertools.combinations(direct_lineage.keys(), 2):
            # If both parents are not None and the same, samples are siblings.
            if (
                direct_lineage[sample_i][DirectRelation.MOTHER]
                and direct_lineage[sample_i][DirectRelation.FATHER]
                and (
                    direct_lineage[sample_i][DirectRelation.MOTHER]
                    == direct_lineage[sample_j][DirectRelation.MOTHER]
                )
                and (
                    direct_lineage[sample_i][DirectRelation.FATHER]
                    == direct_lineage[sample_j][DirectRelation.FATHER]
                )
            ):
                collateral_lineage[sample_i][CollateralRelation.SIBLING] = sample_j

            # If only a single parent is the same, samples are half siblings
            elif (
                direct_lineage[sample_i][DirectRelation.MOTHER]
                == direct_lineage[sample_j][DirectRelation.MOTHER]
            ) or (
                direct_lineage[sample_i][DirectRelation.FATHER]
                == direct_lineage[sample_j][DirectRelation.FATHER]
            ):
                collateral_lineage[sample_i][CollateralRelation.HALF_SIBLING] = sample_j

            # If either set of one sample's grandparents is equal to
            if (
                direct_lineage[sample_i][DirectRelation.MATERNAL_GRANDMOTHER]
                and direct_lineage[sample_i][DirectRelation.MATERNAL_GRANDFATHER]
                and (
                    direct_lineage[sample_i][DirectRelation.MATERNAL_GRANDMOTHER]
                    == direct_lineage[sample_j][DirectRelation.MOTHER]
                )
                and (
                    direct_lineage[sample_i][DirectRelation.MATERNAL_GRANDFATHER]
                    == direct_lineage[sample_j][DirectRelation.FATHER]
                )
            ) or (
                direct_lineage[sample_i][DirectRelation.PATERNAL_GRANDMOTHER]
                and direct_lineage[sample_i][DirectRelation.PATERNAL_GRANDFATHER]
                and (
                    direct_lineage[sample_i][DirectRelation.PATERNAL_GRANDMOTHER]
                    == direct_lineage[sample_j][DirectRelation.MOTHER]
                )
                and (
                    direct_lineage[sample_i][DirectRelation.PATERNAL_GRANDFATHER]
                    == direct_lineage[sample_j][DirectRelation.FATHER]
                )
            ):
                collateral_lineage[sample_i][CollateralRelation.AUNT_UNCLE] = sample_j
        return collateral_lineage

    @classmethod
    def parse(cls, family_guid: str, rows: list[hl.Struct]) -> Family:
        direct_lineage = cls.parse_direct_lineage(rows)
        collateral_lineage = cls.parse_collateral_lineage(direct_lineage)
        return cls(
            family_guid=family_guid,
            collateral_lineage=collateral_lineage,
            direct_lineage=direct_lineage,
        )


def parse_pedigree_ht(
    pedigree_ht: hl.Table,
) -> list[Family]:
    families = []
    for family_guid, rows in itertools.groupby(
        pedigree_ht.collect(),
        lambda x: x.family_guid,
    ):
        families.append(Family.parse(family_guid, list(rows)))
    return families
