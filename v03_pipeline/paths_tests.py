import unittest

from v03_pipeline.constants import DatasetType, Env, ReferenceGenome
from v03_pipeline.paths import (
    family_table_path,
    project_table_path,
    variant_annotations_table_path,
)


class TestPaths(unittest.TestCase):

    def test_family_table_path(self) -> None:
        self.assertEqual(
            family_table_path(Env.DEV, ReferenceGenome.GRCh37, DatasetType.SNV, 'franklin'),
            'gs://seqr-scratch-temp/GRCh37/v03/SNV/families/franklin/all_samples.ht'
        )
        self.assertEqual(
            family_table_path(Env.PROD, ReferenceGenome.GRCh37, DatasetType.SNV, 'franklin'),
            'gs://seqr-loading-temp/GRCh37/v03/SNV/families/franklin/all_samples.ht'
        )

    def test_project_table_path(self) -> None:
        self.assertEqual(
            project_table_path(Env.PROD, ReferenceGenome.GRCh38, DatasetType.MITO, 'R0652_pipeline_test'),
            'gs://seqr-datasets/GRCh38/v03/MITO/projects/R0652_pipeline_test/all_samples.ht'
        )

    def test_variant_annotations_table_path(self) -> None:
        self.assertEqual(
            variant_annotations_table_path(Env.DEV, ReferenceGenome.GRCh38, DatasetType.GCNV),
            'gs://seqr-scratch-temp/GRCh38/v03/GCNV/annotations.ht'
        )
        