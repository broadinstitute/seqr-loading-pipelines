from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from v03_pipeline.lib.model import Ploidy

if TYPE_CHECKING:
    import hail as hl


@dataclass
class Sample:
    sample_id: str
    sex: Ploidy
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
    samples: dict[str, Sample]

    def __hash__(self):
        return hash(self.family_guid)

    @staticmethod
    def parse_direct_lineage(rows: list[hl.Struct]) -> dict[str, Sample]:
        samples = {}
        for row in rows:
            samples[row.s] = Sample(
                sample_id=row.s,
                sex=Ploidy(row.sex),
                mother=row.maternal_s,
                father=row.paternal_s,
            )

        for row in rows:
            # Maternal GrandParents
            maternal_s = samples[row.s].mother
            if maternal_s and samples[maternal_s].mother:
                samples[row.s].maternal_grandmother = samples[maternal_s].mother
            if maternal_s and samples[maternal_s].father:
                samples[row.s].maternal_grandfather = samples[maternal_s].father

            # Paternal GrandParents
            paternal_s = samples[row.s].father
            if paternal_s and samples[paternal_s].mother:
                samples[row.s].paternal_grandmother = samples[paternal_s].mother
            if paternal_s and samples[paternal_s].father:
                samples[row.s].paternal_grandfather = samples[paternal_s].father
        return samples

    @staticmethod
    def parse_collateral_lineage(
        samples: dict[str, Sample],
    ) -> dict[str, Sample]:
        for sample_i, sample_j in itertools.combinations(samples.keys(), 2):
            # If other sample is already related, continue
            if (
                sample_j == samples[sample_i].mother
                or sample_j == samples[sample_i].father
                or sample_j == samples[sample_i].maternal_grandmother
                or sample_j == samples[sample_i].maternal_grandfather
                or sample_j == samples[sample_i].paternal_grandmother
                or sample_j == samples[sample_i].paternal_grandfather
            ):
                continue

            # If both parents are identified and the same, samples are siblings.
            if (
                samples[sample_i].mother
                and samples[sample_i].father
                and (samples[sample_i].mother == samples[sample_j].mother)
                and (samples[sample_i].father == samples[sample_j].father)
            ):
                samples[sample_i].siblings.append(
                    sample_j,
                )
                continue

            # If only a single parent is non-null and the same, samples are half siblings
            if (
                samples[sample_i].mother
                and samples[sample_i].mother == samples[sample_j].mother
            ) or (
                samples[sample_i].father
                and samples[sample_i].father == samples[sample_j].father
            ):
                samples[sample_i].half_siblings.append(
                    sample_j,
                )
                continue

            # If either set of one sample's grandparents is equal the other's parents,
            # they're aunt/uncle
            if (
                samples[sample_i].maternal_grandmother
                and samples[sample_i].maternal_grandfather
                and (samples[sample_i].maternal_grandmother == samples[sample_j].mother)
                and (samples[sample_i].maternal_grandfather == samples[sample_j].father)
            ) or (
                samples[sample_i].paternal_grandmother
                and samples[sample_i].paternal_grandfather
                and (samples[sample_i].paternal_grandmother == samples[sample_j].mother)
                and (samples[sample_i].paternal_grandfather == samples[sample_j].father)
            ):
                samples[sample_i].aunt_uncles.append(
                    sample_j,
                )
        return samples

    @classmethod
    def parse(cls, family_guid: str, rows: list[hl.Struct]) -> Family:
        samples = cls.parse_direct_lineage(rows)
        samples = cls.parse_collateral_lineage(samples)
        return cls(
            family_guid=family_guid,
            samples=samples,
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
