import unittest

from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome, Storage
from v03_pipeline.lib.paths import (
    family_table_path,
    project_table_path,
    variant_annotations_table_path,
    variant_lookup_table_path,
)


class TestPaths(unittest.TestCase):
    def test_family_table_path(self) -> None:
        for env, expected_path in [
            (Env.TEST, 'test-datasets/GRCh37/v03/SNV/families/franklin/samples.ht'),
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
                Storage.PERMANENT,
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                'R0652_pipeline_test',
            ),
            'gs://seqr-datasets/GRCh38/v03/MITO/projects/R0652_pipeline_test/samples.ht',
        )
        self.assertEqual(
            project_table_path(
                Env.PROD,
                Storage.CHECKPOINT,
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                'R0652_pipeline_test',
            ),
            'gs://seqr-scratch-temp/checkpoints/GRCh38/v03/MITO/projects/R0652_pipeline_test/samples.ht',
        )

    def test_variant_annotations_table_path(self) -> None:
        self.assertEqual(
            variant_annotations_table_path(
                Env.DEV,
                Storage.PERMANENT,
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
            ),
            'gs://seqr-scratch-temp/GRCh38/v03/GCNV/annotations.ht',
        )
        self.assertEqual(
            variant_annotations_table_path(
                Env.DEV,
                Storage.CHECKPOINT,
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
            ),
            'gs://seqr-scratch-temp/checkpoints/GRCh38/v03/GCNV/annotations.ht',
        )

    def test_variant_lookup_table_path(self) -> None:
        self.assertEqual(
            variant_lookup_table_path(
                Env.LOCAL,
                Storage.PERMANENT,
                ReferenceGenome.GRCh37,
                DatasetType.SV,
            ),
            'seqr-datasets/GRCh37/v03/SV/lookup.ht',
        )
        self.assertEqual(
            variant_lookup_table_path(
                Env.LOCAL,
                Storage.CHECKPOINT,
                ReferenceGenome.GRCh37,
                DatasetType.SV,
            ),
            'seqr-datasets/checkpoints/GRCh37/v03/SV/lookup.ht',
        )
