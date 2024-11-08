from enum import Enum

import hail as hl

from v03_pipeline.lib.model import AccessControl, DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.reference_data.cadd import load_cadd_ht_from_raw_dataset
from v03_pipeline.lib.reference_data.hgmd import download_and_import_hgmd_vcf


class ReferenceDataset(str, Enum):
    cadd = 'cadd'
    hgmd = 'hgmd'

    @classmethod
    def for_reference_genome_dataset_type(
        cls,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
    ) -> list['ReferenceDataset']:
        reference_datasets = {
            (ReferenceGenome.GRCh37, DatasetType.SNV_INDEL): [
                ReferenceDataset.cadd,
                ReferenceDataset.hgmd,
            ],
            (ReferenceGenome.GRCh38, DatasetType.SNV_INDEL): [
                ReferenceDataset.cadd,
                ReferenceDataset.hgmd,
            ],
        }.get((reference_genome, dataset_type), [])
        if not Env.ACCESS_PRIVATE_REFERENCE_DATASETS:
            return [
                rd
                for rd in reference_datasets
                if rd.access_control == AccessControl.PUBLIC
            ]
        return reference_datasets

    @property
    def access_control(self) -> AccessControl:
        if self == ReferenceDataset.hgmd:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    def version(self, reference_genome: ReferenceGenome):
        return {
            (ReferenceDataset.cadd, ReferenceGenome.GRCh37): 'v1.7',
            (ReferenceDataset.cadd, ReferenceGenome.GRCh38): 'v1.7',
            (ReferenceDataset.hgmd, ReferenceGenome.GRCh37): 'HGMD_Pro_2023',
            (ReferenceDataset.hgmd, ReferenceGenome.GRCh38): 'HGMD_Pro_2023',
        }[self, reference_genome]

    def raw_dataset_path(self, reference_genome: ReferenceGenome):
        return {
            (ReferenceDataset.cadd, ReferenceGenome.GRCh37): {
                'snv': 'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh37/whole_genome_SNVs.tsv.gz',
                'indel': 'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh38/gnomad.genomes.r4.0.indel.tsv.gz',
            },
            (ReferenceDataset.cadd, ReferenceGenome.GRCh38): {
                'snv': 'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh38/whole_genome_SNVs.tsv.gz',
                'indel': 'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh37/gnomad.genomes-exomes.r4.0.indel.tsv.gz',
            },
            (
                ReferenceDataset.hgmd,
                ReferenceGenome.GRCh37,
            ): 'gs://seqr-reference-data-private/GRCh37/HGMD/HGMD_Pro_2023.1_hg19.vcf.gz',
            (
                ReferenceDataset.hgmd,
                ReferenceGenome.GRCh38,
            ): 'gs://seqr-reference-data-private/GRCh38/HGMD/HGMD_Pro_2023.1_hg38.vcf.gz',
        }[self, reference_genome]

    def load_parsed_dataset_func(self):
        return {
            ReferenceDataset.cadd: load_cadd_ht_from_raw_dataset,
            ReferenceDataset.hgmd: download_and_import_hgmd_vcf,
        }[self]

    @staticmethod
    def table_key_type(reference_genome: ReferenceGenome):
        return hl.tstruct(
            locus=hl.tlocus(reference_genome.value),
            alleles=hl.tarray(hl.tstr),
        )

    def select(self, reference_genome: ReferenceGenome):
        """
        Optional list of fields to select or dict of new field name to location of old field in the reference dataset.
        If '#' is at the end, we know to select the appropriate biallelic using the a_index.
        """
        return {
            (ReferenceDataset.cadd, ReferenceGenome.GRCh37): ['PHRED'],
            (ReferenceDataset.cadd, ReferenceGenome.GRCh38): ['PHRED'],
            (ReferenceDataset.hgmd, ReferenceGenome.GRCh37): {
                'accession': 'rsid',
                'class': 'info.CLASS',
            },
            (ReferenceDataset.hgmd, ReferenceGenome.GRCh38): {
                'accession': 'rsid',
                'class': 'info.CLASS',
            },
        }.get((self, reference_genome))

    def custom_select(self):
        """Optional custom select function."""

    def enum_select(self, reference_genome: ReferenceGenome):
        """Optional dictionary mapping field_name to a list of enumerated values."""
        return {
            (ReferenceDataset.hgmd, ReferenceGenome.GRCh37): {
                'class': ['DM', 'DM?', 'DP', 'DFP', 'FP', 'R'],
            },
            (ReferenceDataset.hgmd, ReferenceGenome.GRCh38): {
                'class': ['DM', 'DM?', 'DP', 'DFP', 'FP', 'R'],
            },
        }.get((self, reference_genome))
