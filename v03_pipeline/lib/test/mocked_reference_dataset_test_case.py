import shutil

import responses

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.test.mock_clinvar_urls import mock_clinvar_urls
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_DBNSFP_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/dbnsfp/1.0.ht'
TEST_EIGEN_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/eigen/1.0.ht'
TEST_CLINVAR_38_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh38/clinvar/2024-11-11.ht'
)
TEST_EXAC_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/exac/1.0.ht'
TEST_SPLICE_AI_38_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh38/splice_ai/1.0.ht'
)
TEST_TOPMED_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/topmed/1.0.ht'
TEST_HGMD_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/hgmd/1.0.ht'
TEST_GNOMAD_EXOMES_38_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh38/gnomad_exomes/1.0.ht'
)
TEST_GNOMAD_GENOMES_38_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh38/gnomad_genomes/1.0.ht'
)
TEST_GNOMAD_NONCODING_CONSTRAINT_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/gnomad_non_coding_constraint/1.0.ht'
TEST_SCREEN_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/screen/1.0.ht'
TEST_HELIX_MITO_38_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh38/helix_mito/1.0.ht'
)
TEST_HMTVAR_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/hmtvar/1.0.ht'
TEST_MITIMPACT_38_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh38/mitimpact/1.0.ht'
)
TEST_MITOMAP_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/mitomap/1.0.ht'
TEST_GNOMAD_MITO_38_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh38/gnomad_mito/1.0.ht'
)
TEST_LOCAL_CONSTRAINT_MITO_38_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh38/local_constraint_mito/1.0.ht'
)

TEST_DBNSFP_37_HT = 'v03_pipeline/var/test/reference_datasets/GRCh37/dbnsfp/1.0.ht'
TEST_EIGEN_37_HT = 'v03_pipeline/var/test/reference_datasets/GRCh37/eigen/1.0.ht'
TEST_CLINVAR_37_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh37/clinvar/2024-11-11.ht'
)
TEST_EXAC_37_HT = 'v03_pipeline/var/test/reference_datasets/GRCh37/exac/1.0.ht'
TEST_SPLICE_AI_37_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh37/splice_ai/1.0.ht'
)
TEST_TOPMED_37_HT = 'v03_pipeline/var/test/reference_datasets/GRCh37/topmed/1.0.ht'
TEST_HGMD_37_HT = 'v03_pipeline/var/test/reference_datasets/GRCh37/hgmd/1.0.ht'
TEST_GNOMAD_EXOMES_37_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh37/gnomad_exomes/1.0.ht'
)
TEST_GNOMAD_GENOMES_37_HT = (
    'v03_pipeline/var/test/reference_datasets/GRCh37/gnomad_genomes/1.0.ht'
)


class MockedReferenceDataTestCase(MockedDatarootTestCase):
    @responses.activate
    def setUp(self) -> None:
        super().setUp()
        shutil.copytree(
            TEST_DBNSFP_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.dbnsfp,
            ),
        )
        shutil.copytree(
            TEST_EIGEN_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.eigen,
            ),
        )
        with mock_clinvar_urls():
            shutil.copytree(
                TEST_CLINVAR_38_HT,
                valid_reference_dataset_path(
                    ReferenceGenome.GRCh38,
                    ReferenceDataset.clinvar,
                ),
            )
        shutil.copytree(
            TEST_EXAC_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.exac,
            ),
        )
        shutil.copytree(
            TEST_SPLICE_AI_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.splice_ai,
            ),
        )
        shutil.copytree(
            TEST_TOPMED_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.topmed,
            ),
        )
        shutil.copytree(
            TEST_HGMD_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.hgmd,
            ),
        )
        shutil.copytree(
            TEST_GNOMAD_EXOMES_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.gnomad_exomes,
            ),
        )
        shutil.copytree(
            TEST_GNOMAD_GENOMES_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.gnomad_genomes,
            ),
        )
        shutil.copytree(
            TEST_GNOMAD_NONCODING_CONSTRAINT_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.gnomad_non_coding_constraint,
            ),
        )
        shutil.copytree(
            TEST_SCREEN_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.screen,
            ),
        )
        shutil.copytree(
            TEST_HELIX_MITO_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.helix_mito,
            ),
        )
        shutil.copytree(
            TEST_HMTVAR_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.hmtvar,
            ),
        )
        shutil.copytree(
            TEST_MITIMPACT_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.mitimpact,
            ),
        )
        shutil.copytree(
            TEST_MITOMAP_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.mitomap,
            ),
        )
        shutil.copytree(
            TEST_GNOMAD_MITO_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.gnomad_mito,
            ),
        )
        shutil.copytree(
            TEST_LOCAL_CONSTRAINT_MITO_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.local_constraint_mito,
            ),
        )
        shutil.copytree(
            TEST_DBNSFP_37_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh37,
                ReferenceDataset.dbnsfp,
            ),
        )
        shutil.copytree(
            TEST_EIGEN_37_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh37,
                ReferenceDataset.eigen,
            ),
        )
        with mock_clinvar_urls(ReferenceGenome.GRCh37):
            shutil.copytree(
                TEST_CLINVAR_37_HT,
                valid_reference_dataset_path(
                    ReferenceGenome.GRCh37,
                    ReferenceDataset.clinvar,
                ),
            )
        shutil.copytree(
            TEST_EXAC_37_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh37,
                ReferenceDataset.exac,
            ),
        )
        shutil.copytree(
            TEST_SPLICE_AI_37_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh37,
                ReferenceDataset.splice_ai,
            ),
        )
        shutil.copytree(
            TEST_TOPMED_37_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh37,
                ReferenceDataset.topmed,
            ),
        )
        shutil.copytree(
            TEST_HGMD_37_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh37,
                ReferenceDataset.hgmd,
            ),
        )
        shutil.copytree(
            TEST_GNOMAD_EXOMES_37_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh37,
                ReferenceDataset.gnomad_exomes,
            ),
        )
        shutil.copytree(
            TEST_GNOMAD_GENOMES_37_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh37,
                ReferenceDataset.gnomad_genomes,
            ),
        )
