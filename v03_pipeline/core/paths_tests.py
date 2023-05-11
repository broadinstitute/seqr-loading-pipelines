import unittest

from v03_pipeline.core.definitions import (
    DatasetType,
    Env,
    ReferenceDatasetCollection,
    ReferenceGenome,
    SampleSource,
    SampleType,
    ValidationDatasetCollection,
)
from v03_pipeline.core.paths import (
    family_table_path,
    project_pedigree_path,
    project_remap_path,
    project_subset_path,
    project_table_path,
    reference_dataset_collection_path,
    validation_dataset_collection_path,
    variant_annotations_table_path,
    variant_lookup_table_path,
)


class TestPaths(unittest.TestCase):
    def test_family_table_path(self) -> None:
        self.assertEqual(
            family_table_path(
                Env.LOCAL,
                ReferenceGenome.GRCh37,
                DatasetType.SNV,
                'franklin',
            ),
            '/var/seqr-datasets/GRCh37/v03/snv/families/franklin/all_samples.ht',
        )
        self.assertEqual(
            family_table_path(
                Env.DEV,
                ReferenceGenome.GRCh37,
                DatasetType.SNV,
                'franklin',
            ),
            'gs://seqr-scratch-temp/GRCh37/v03/snv/families/franklin/all_samples.ht',
        )
        self.assertEqual(
            family_table_path(
                Env.PROD,
                ReferenceGenome.GRCh37,
                DatasetType.SNV,
                'franklin',
            ),
            'gs://seqr-loading-temp/GRCh37/v03/snv/families/franklin/all_samples.ht',
        )

    def test_project_pedigree_path(self) -> None:
        self.assertEqual(
            project_pedigree_path(
                ReferenceGenome.GRCh38,
                SampleSource.ANVIL,
                SampleType.WES,
                '123_abc',
            ),
            'gs://seqr-datasets/v02/GRCh38/AnVIL_WES/base/projects/123_abc/123_abc_pedigree.tsv',
        )

    def test_project_remap_path(self) -> None:
        self.assertEqual(
            project_remap_path(
                ReferenceGenome.GRCh37,
                SampleSource.RDG_BROAD_INTERNAL,
                SampleType.WGS,
                '015_test',
            ),
            'gs://seqr-datasets/v02/GRCh37/RDG_WGS_Broad_Internal/base/projects/015_test/015_test_remap.tsv',
        )

    def test_project_table_path(self) -> None:
        self.assertEqual(
            project_table_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                'R0652_pipeline_test',
            ),
            'gs://seqr-datasets/GRCh38/v03/mito/projects/R0652_pipeline_test/all_samples.ht',
        )

    def test_project_subset_path(self) -> None:
        self.assertEqual(
            project_subset_path(
                ReferenceGenome.GRCh38,
                SampleSource.RDG_BROAD_INTERNAL,
                SampleType.WES,
                '015_test',
            ),
            'gs://seqr-datasets/v02/GRCh38/RDG_WES_Broad_Internal/base/projects/015_test/015_test_ids.txt',
        )

    def test_reference_dataset_collection_path(self) -> None:
        self.assertEqual(
            reference_dataset_collection_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                ReferenceDatasetCollection.HGMD,
                '1.1.1',
            ),
            'gs://seqr-reference-data-private/GRCh38/v03/hgmd/1.1.1.ht',
        )
        self.assertEqual(
            reference_dataset_collection_path(
                Env.DEV,
                ReferenceGenome.GRCh37,
                ReferenceDatasetCollection.CLINVAR,
                '1.2.3',
            ),
            'gs://seqr-scratch-temp/GRCh37/v03/clinvar/1.2.3.ht',
        )

    def test_validation_dataset_collection_path(self) -> None:
        self.assertEqual(
            validation_dataset_collection_path(
                Env.LOCAL,
                ReferenceGenome.GRCh37,
                ValidationDatasetCollection.SAMPLE_TYPE_VALIDATION,
                '3.2.1',
            ),
            '/var/seqr-reference-data/GRCh37/v03/sample_type_validation/3.2.1.ht',
        )
        self.assertEqual(
            validation_dataset_collection_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                ValidationDatasetCollection.SAMPLE_TYPE_VALIDATION,
                '3.2.1',
            ),
            'gs://seqr-reference-data/GRCh38/v03/sample_type_validation/3.2.1.ht',
        )

    def test_variant_annotations_table_path(self) -> None:
        self.assertEqual(
            variant_annotations_table_path(
                Env.DEV,
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
            ),
            'gs://seqr-scratch-temp/GRCh38/v03/gcnv/annotations.ht',
        )

    def test_variant_lookup_table_path(self) -> None:
        self.assertEqual(
            variant_lookup_table_path(
                Env.PROD,
                ReferenceGenome.GRCh37,
                DatasetType.SV,
            ),
            'gs://seqr-datasets/GRCh37/v03/sv/lookup.ht',
        )