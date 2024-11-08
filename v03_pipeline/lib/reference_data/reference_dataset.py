from enum import Enum

import hail as hl

from v03_pipeline.lib.model import AccessControl, ReferenceGenome
from v03_pipeline.lib.reference_data.cadd import load_cadd_ht_from_raw_dataset


class ReferenceDataset(str, Enum):
    cadd = 'cadd'

    @property
    def access_control(self) -> AccessControl:
        if self == ReferenceDataset.cadd:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    def version(self, reference_genome: ReferenceGenome):
        return {
            (ReferenceDataset.cadd, ReferenceGenome.GRCh37): 'v1.7',
            (ReferenceDataset.cadd, ReferenceGenome.GRCh38): 'v1.7',
        }[self, reference_genome]

    def raw_dataset_path(self, reference_genome: ReferenceGenome):
        return {
            (
                ReferenceDataset.cadd,
                ReferenceGenome.GRCh37,
            ): {
                'snv': 'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh37/whole_genome_SNVs.tsv.gz',
                'indel': 'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh38/gnomad.genomes.r4.0.indel.tsv.gz',
            },
            (
                ReferenceDataset.cadd,
                ReferenceGenome.GRCh38,
            ): {
                'snv': 'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh38/whole_genome_SNVs.tsv.gz',
                'indel': 'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh37/gnomad.genomes-exomes.r4.0.indel.tsv.gz',
            },
        }[self, reference_genome]

    def load_parsed_dataset_func(self):
        return {
            ReferenceDataset.cadd: load_cadd_ht_from_raw_dataset,
        }[self]

    @staticmethod
    def table_key_type(reference_genome: ReferenceGenome):
        return hl.tstruct(
            locus=hl.tlocus(reference_genome.value),
            alleles=hl.tarray(hl.tstr),
        )
