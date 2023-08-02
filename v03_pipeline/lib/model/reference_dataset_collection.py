from __future__ import annotations

from enum import Enum

import hail as hl

from v03_pipeline.lib.model.definitions import AccessControl, ReferenceGenome


class ReferenceDatasetCollection(Enum):
    COMBINED = 'combined'
    COMBINED_MITO = 'combined_mito'
    HGMD = 'hgmd'
    INTERVAL = 'interval'
    INTERVAL_MITO = 'interval_mito'

    @property
    def access_control(self) -> AccessControl:
        if self == ReferenceDatasetCollection.HGMD:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    @property
    def datasets(self) -> list[str]:
        return {
            ReferenceDatasetCollection.COMBINED: [
                'cadd',
                'clinvar',
                'dbnsfp',
                'eigen',
                'exac',
                'gnomad_exomes',
                'gnomad_genomes',
                'mpc',
                'primate_ai',
                'splice_ai',
                'topmed',
            ],
            ReferenceDatasetCollection.COMBINED_MITO: [
                'clinvar',
                'dbnsfp_mito',
                'gnomad_mito',
                'helix_mito',
                'hmtvar',
                'mitomap',
                'mitimpact',
            ],
            ReferenceDatasetCollection.HGMD: ['hgmd'],
            ReferenceDatasetCollection.INTERVAL: [
                'gnomad_non_coding_constraint',
                'screen',
            ],
            ReferenceDatasetCollection.INTERVAL_MITO: [
                'high_constraint_region_mito',
            ],
        }[self]

    def table_key_type(
        self,
        reference_genome: ReferenceGenome,
    ) -> hl.tstruct:
        default_key = hl.tstruct(
            locus=hl.tlocus(reference_genome.value),
            alleles=hl.tarray(hl.tstr),
        )
        return {
            ReferenceDatasetCollection.INTERVAL: hl.tstruct(
                interval=hl.tinterval(hl.tlocus(reference_genome.value)),
            ),
            ReferenceDatasetCollection.INTERVAL_MITO: hl.tstruct(
                interval=hl.tinterval(hl.tlocus(reference_genome.value)),
            ),
        }.get(self, default_key)
