import importlib
import types
from collections.abc import Callable
from enum import StrEnum
from typing import Union

import hail as hl

from v03_pipeline.lib.misc.validation import (
    validate_allele_type,
    validate_no_duplicate_variants,
)
from v03_pipeline.lib.model import (
    AccessControl,
    DatasetType,
    FeatureFlag,
    ReferenceGenome,
)
from v03_pipeline.lib.reference_datasets import clinvar, dbnsfp
from v03_pipeline.lib.reference_datasets.misc import (
    compress_floats,
    filter_contigs,
    filter_mito_contigs,
    get_enum_select_fields,
)

DATASET_TYPES = 'dataset_types'
ENUMS = 'enums'
EXCLUDE_FROM_ANNOTATIONS = 'exclude_from_annotations'
FILTER = 'filter'
IS_INTERVAL = 'is_interval'
SELECT = 'select'
VERSION = 'version'
PATH = 'path'


class BaseReferenceDataset:
    @classmethod
    def for_reference_genome_dataset_type(
        cls,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
    ) -> set[Union['ReferenceDataset', 'ReferenceDatasetQuery']]:
        reference_datasets = [
            dataset
            for dataset, config in CONFIG.items()
            if dataset_type in config.get(reference_genome, {}).get(DATASET_TYPES, [])
        ]
        if not FeatureFlag.ACCESS_PRIVATE_REFERENCE_DATASETS:
            return {
                dataset
                for dataset in reference_datasets
                if dataset.access_control == AccessControl.PUBLIC
            }
        return set(reference_datasets)

    @classmethod
    def for_reference_genome_dataset_type_annotations(
        cls,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
    ) -> set['ReferenceDataset']:
        return {
            dataset
            for dataset in cls.for_reference_genome_dataset_type(
                reference_genome,
                dataset_type,
            )
            if not CONFIG[dataset].get(EXCLUDE_FROM_ANNOTATIONS, False)
        }

    @property
    def is_keyed_by_interval(self) -> bool:
        return CONFIG[self].get(IS_INTERVAL, False)

    @property
    def access_control(self) -> AccessControl:
        if self == ReferenceDataset.hgmd:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    def version(self, reference_genome: ReferenceGenome) -> str:
        version = CONFIG[self][reference_genome][VERSION]
        if isinstance(version, types.FunctionType):
            return version(
                self.path(reference_genome),
            )
        return version

    @property
    def enums(self) -> dict | None:
        return CONFIG[self].get(ENUMS)

    @property
    def enum_globals(self) -> hl.Struct:
        if self.enums:
            return hl.Struct(**self.enums)
        return hl.Struct()

    @property
    def filter(  # noqa: A003
        self,
    ) -> Callable[[ReferenceGenome, DatasetType, hl.Table], hl.Table] | None:
        return CONFIG[self].get(FILTER)

    @property
    def select(
        self,
    ) -> Callable[[ReferenceGenome, DatasetType, hl.Table], hl.Table] | None:
        return CONFIG[self].get(SELECT)

    def path(self, reference_genome: ReferenceGenome) -> str | list[str]:
        return CONFIG[self][reference_genome][PATH]

    def get_ht(
        self,
        reference_genome: ReferenceGenome,
    ) -> hl.Table:
        module = importlib.import_module(
            f'v03_pipeline.lib.reference_datasets.{self.name}',
        )
        path = self.path(reference_genome)
        ht = module.get_ht(path, reference_genome)
        ht = compress_floats(ht)
        enum_selects = get_enum_select_fields(ht, self.enums)
        if enum_selects:
            ht = ht.transmute(**enum_selects)
        ht = filter_contigs(ht, reference_genome)
        # Reference Datasets are DatasetType agnostic, but these
        # methods (in theory) support SV/GCNV.  SNV_INDEL
        # is passed as a proxy for non-SV/GCNV.
        validate_allele_type(ht, DatasetType.SNV_INDEL)
        validate_no_duplicate_variants(ht, reference_genome, DatasetType.SNV_INDEL)
        # NB: we do not filter with "filter" here
        # ReferenceDatasets are DatasetType agnostic and that
        # filter is only used at annotation time.
        return ht.annotate_globals(
            version=self.version(reference_genome),
            enums=self.enum_globals,
        )


class ReferenceDataset(BaseReferenceDataset, StrEnum):
    clinvar = 'clinvar'
    dbnsfp = 'dbnsfp'
    exac = 'exac'
    eigen = 'eigen'
    helix_mito = 'helix_mito'
    hgmd = 'hgmd'
    hmtvar = 'hmtvar'
    mitimpact = 'mitimpact'
    splice_ai = 'splice_ai'
    topmed = 'topmed'
    gnomad_coding_and_noncoding = 'gnomad_coding_and_noncoding'
    gnomad_exomes = 'gnomad_exomes'
    gnomad_genomes = 'gnomad_genomes'
    gnomad_qc = 'gnomad_qc'
    gnomad_mito = 'gnomad_mito'
    gnomad_non_coding_constraint = 'gnomad_non_coding_constraint'
    screen = 'screen'
    local_constraint_mito = 'local_constraint_mito'
    mitomap = 'mitomap'


class ReferenceDatasetQuery(BaseReferenceDataset, StrEnum):
    clinvar_path_variants = 'clinvar_path_variants'
    high_af_variants = 'high_af_variants'

    @classmethod
    def for_reference_genome_dataset_type(
        cls,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
    ) -> set['ReferenceDatasetQuery']:
        return {
            dataset
            for dataset in super().for_reference_genome_dataset_type(
                reference_genome,
                dataset_type,
            )
            if isinstance(dataset, cls)
        }

    @property
    def requires(self) -> ReferenceDataset:
        return {
            self.clinvar_path_variants: ReferenceDataset.clinvar,
            self.high_af_variants: ReferenceDataset.gnomad_genomes,
        }[self]

    def get_ht(
        self,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
        reference_dataset_ht: hl.Table,
    ) -> hl.Table:
        module = importlib.import_module(
            f'v03_pipeline.lib.reference_datasets.{self.name}',
        )
        ht = module.get_ht(reference_dataset_ht)
        if self.filter:
            ht = self.filter(reference_genome, dataset_type, ht)
        return ht.annotate_globals(
            version=self.version(reference_genome),
        )


CONFIG = {
    ReferenceDataset.dbnsfp: {
        ENUMS: {
            'MutationTaster_pred': ['D', 'A', 'N', 'P'],
        },
        FILTER: filter_mito_contigs,
        SELECT: dbnsfp.select,
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'https://dbnsfp.s3.amazonaws.com/dbNSFP4.7a.zip',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL, DatasetType.MITO]),
            VERSION: '1.0',
            PATH: 'https://dbnsfp.s3.amazonaws.com/dbNSFP4.7a.zip',
        },
    },
    ReferenceDataset.eigen: {
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.1',
            # NB: The download link on the Eigen website (http://www.columbia.edu/~ii2135/download.html) is broken
            # as of 11/15/24 so we will host the data
            PATH: 'gs://seqr-reference-data/GRCh37/eigen/EIGEN_coding_noncoding.grch37.ht',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.1',
            PATH: 'gs://seqr-reference-data/GRCh38/eigen/EIGEN_coding_noncoding.liftover_grch38.ht',
        },
    },
    ReferenceDataset.clinvar: {
        ENUMS: clinvar.ENUMS,
        FILTER: filter_mito_contigs,
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: clinvar.parse_clinvar_release_date,
            PATH: 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL, DatasetType.MITO]),
            VERSION: clinvar.parse_clinvar_release_date,
            PATH: 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
        },
    },
    ReferenceDataset.exac: {
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.1',
            PATH: 'gs://gcp-public-data--gnomad/legacy/exacv1_downloads/release1/ExAC.r1.sites.vep.vcf.gz',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.1',
            # NB: Exac is only available on GRCh37 so we host a lifted over version
            PATH: 'gs://seqr-reference-data/GRCh38/gnomad/ExAC.r1.sites.liftover.b38.vcf.gz',
        },
    },
    ReferenceDataset.helix_mito: {
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.MITO]),
            VERSION: '1.0',
            PATH: 'https://helix-research-public.s3.amazonaws.com/mito/HelixMTdb_20200327.tsv',
        },
    },
    ReferenceDataset.splice_ai: {
        ENUMS: {
            'splice_consequence': [
                'Acceptor gain',
                'Acceptor loss',
                'Donor gain',
                'Donor loss',
                'No consequence',
            ],
        },
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.1',
            PATH: [
                'gs://seqr-reference-data/GRCh37/spliceai/spliceai_scores.masked.snv.hg19.vcf.gz',
                'gs://seqr-reference-data/GRCh37/spliceai/spliceai_scores.masked.indel.hg19.vcf.gz',
            ],
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.1',
            # NB: SpliceAI data is only available to download for authenticated Illumina users, so we will host the data
            PATH: [
                'gs://seqr-reference-data/GRCh38/spliceai/spliceai_scores.masked.snv.hg38.vcf.gz',
                'gs://seqr-reference-data/GRCh38/spliceai/spliceai_scores.masked.indel.hg38.vcf.gz',
            ],
        },
    },
    ReferenceDataset.topmed: {
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.1',
            PATH: 'gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.vcf.gz',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.1',
            # NB: TopMed data is available to download via https://legacy.bravo.sph.umich.edu/freeze8/hg38/downloads/vcf/<chrom>
            # However, users must be authenticated and accept TOS to access it so for now we will host a copy of the data
            PATH: 'gs://seqr-reference-data/GRCh38/TopMed/bravo-dbsnp-all.vcf.gz',
        },
    },
    ReferenceDataset.hmtvar: {
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.MITO]),
            VERSION: '1.1',
            #  NB: https://www.hmtvar.uniba.it is unavailable as of 11/15/24 so we will host the data
            PATH: 'https://storage.googleapis.com/seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar%20Jan.%2010%202022.json',
        },
    },
    ReferenceDataset.mitimpact: {
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.MITO]),
            VERSION: '1.0',
            PATH: 'https://mitimpact.css-mendel.it/cdn/MitImpact_db_3.1.3.txt.zip',
        },
    },
    ReferenceDataset.hgmd: {
        ENUMS: {'class': ['DM', 'DM?', 'DP', 'DFP', 'FP', 'R']},
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'gs://seqr-reference-data-private/GRCh37/HGMD/HGMD_Pro_2023.1_hg19.vcf.gz',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'gs://seqr-reference-data-private/GRCh38/HGMD/HGMD_Pro_2023.1_hg38.vcf.gz',
        },
    },
    ReferenceDataset.gnomad_exomes: {
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'gs://gcp-public-data--gnomad/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht',
        },
    },
    ReferenceDataset.gnomad_genomes: {
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'gs://gcp-public-data--gnomad/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'gs://gcp-public-data--gnomad/release/4.1/ht/genomes/gnomad.genomes.v4.1.sites.ht',
        },
    },
    ReferenceDataset.gnomad_qc: {
        EXCLUDE_FROM_ANNOTATIONS: True,
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'gs://seqr-reference-data/gnomad_qc/GRCh37/gnomad.joint.high_callrate_common_biallelic_snps.pruned.mt',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'gs://gcp-public-data--gnomad/release/4.0/pca/gnomad.v4.0.pca_loadings.ht',
        },
    },
    ReferenceDataset.mitomap: {
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.MITO]),
            VERSION: '1.0',
            # Downloaded via https://www.mitomap.org/foswiki/bin/view/MITOMAP/ConfirmedMutations
            PATH: 'gs://seqr-reference-data/GRCh38/mitochondrial/MITOMAP/mitomap_confirmed_mutations_nov_2024.csv',
        },
    },
    ReferenceDataset.gnomad_mito: {
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.MITO]),
            VERSION: '1.1',
            PATH: 'gs://gcp-public-data--gnomad/release/3.1/ht/genomes/gnomad.genomes.v3.1.sites.chrM.ht',
        },
    },
    ReferenceDataset.gnomad_non_coding_constraint: {
        IS_INTERVAL: True,
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'gs://gcp-public-data--gnomad/release/3.1/secondary_analyses/genomic_constraint/constraint_z_genome_1kb.qc.download.txt.gz',
        },
    },
    ReferenceDataset.screen: {
        ENUMS: {
            'region_type': [
                'CTCF-bound',
                'CTCF-only',
                'DNase-H3K4me3',
                'PLS',
                'dELS',
                'pELS',
                'DNase-only',
                'low-DNase',
            ],
        },
        IS_INTERVAL: True,
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            PATH: 'https://downloads.wenglab.org/V3/GRCh38-cCREs.bed',
        },
    },
    ReferenceDataset.local_constraint_mito: {
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.MITO]),
            VERSION: '1.0',
            PATH: 'https://www.biorxiv.org/content/biorxiv/early/2023/01/27/2022.12.16.520778/DC3/embed/media-3.zip',
        },
    },
}
CONFIG[ReferenceDatasetQuery.clinvar_path_variants] = {
    EXCLUDE_FROM_ANNOTATIONS: True,
    **CONFIG[ReferenceDataset.clinvar],
}
CONFIG[ReferenceDataset.gnomad_coding_and_noncoding] = {
    EXCLUDE_FROM_ANNOTATIONS: True,
    **CONFIG[ReferenceDataset.gnomad_genomes],
}
CONFIG[ReferenceDatasetQuery.high_af_variants] = {
    EXCLUDE_FROM_ANNOTATIONS: True,
    **CONFIG[ReferenceDataset.gnomad_genomes],
}
