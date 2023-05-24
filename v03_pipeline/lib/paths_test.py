import unittest

from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceDatasetCollection, ReferenceGenome
from v03_pipeline.lib.paths import (
    family_table_path,
    project_table_path,
    reference_dataset_collection_path,
    variant_annotations_table_path,
    variant_lookup_table_path,
)


class TestPaths(unittest.TestCase):
    def test_family_table_path(self) -> None:
        for env, expected_path in [
            (Env.TEST, 'seqr-datasets/GRCh37/v03/SNV/families/franklin/samples.ht'),
            (Env.LOCAL, 'seqr-datasets/GRCh37/v03/SNV/families/franklin/samples.ht'),
            (
                Env.DEV,
                'gs://seqr-scratch-temp/GRCh37/v03/SNV/families/franklin/samples.ht',
            ),
            (
                Env.PROD,
                'gs://seqr-loading-temp/GRCh37/v03/SNV/families/franklin/samples.ht',
            ),
        ]:
            self.assertEqual(
                family_table_path(
                    env,
                    ReferenceGenome.GRCh37,
                    DatasetType.SNV,
                    'franklin',
                ),
                expected_path,
            )

    def test_project_table_path(self) -> None:
        self.assertEqual(
            project_table_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                'R0652_pipeline_test',
            ),
            'gs://seqr-datasets/GRCh38/v03/MITO/projects/R0652_pipeline_test/samples.ht',
        )

    def test_reference_dataset_collection_path(self) -> None:
        self.assertEqual(
            reference_dataset_collection_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                ReferenceDatasetCollection.HGMD,
            ),
            'gs://seqr-reference-data-private/GRCh38/v03/hgmd.ht',
        )
        self.assertEqual(
            reference_dataset_collection_path(
                Env.DEV,
                ReferenceGenome.GRCh37,
                ReferenceDatasetCollection.COMBINED,
            ),
            'gs://seqr-scratch-temp/GRCh37/v03/combined.ht',
        )
        self.assertEqual(
            reference_dataset_collection_path(
                Env.LOCAL,
                ReferenceGenome.GRCh37,
                ReferenceDatasetCollection.HGMD,
            ),
            'seqr-reference-data/GRCh37/v03/hgmd.ht',
        )

    def test_variant_annotations_table_path(self) -> None:
        self.assertEqual(
            variant_annotations_table_path(
                Env.DEV,
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
            ),
            'gs://seqr-scratch-temp/GRCh38/v03/GCNV/annotations.ht',
        )

    def test_variant_lookup_table_path(self) -> None:
        self.assertEqual(
            variant_lookup_table_path(
                Env.LOCAL,
                ReferenceGenome.GRCh37,
                DatasetType.SV,
            ),
            'seqr-datasets/GRCh37/v03/SV/lookup.ht',
        )
