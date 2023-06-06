import unittest

from v03_pipeline.lib.model import (
    DatasetType,
    Env,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import (
    family_table_path,
    project_table_path,
    remapped_and_subsetted_callset_path,
    sample_lookup_table_path,
    valid_reference_dataset_collection_path,
    variant_annotations_table_path,
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

    def test_valid_reference_dataset_collection_path(self) -> None:
        self.assertEqual(
            valid_reference_dataset_collection_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                ReferenceDatasetCollection.HGMD,
            ),
            'gs://seqr-reference-data-private/GRCh38/v03/hgmd.ht',
        )
        self.assertEqual(
            valid_reference_dataset_collection_path(
                Env.DEV,
                ReferenceGenome.GRCh37,
                ReferenceDatasetCollection.COMBINED,
            ),
            'gs://seqr-scratch-temp/GRCh37/v03/combined.ht',
        )
        self.assertEqual(
            valid_reference_dataset_collection_path(
                Env.LOCAL,
                ReferenceGenome.GRCh37,
                ReferenceDatasetCollection.HGMD,
            ),
            None,
        )

    def test_sample_lookup_table_path(self) -> None:
        self.assertEqual(
            sample_lookup_table_path(
                Env.LOCAL,
                ReferenceGenome.GRCh37,
                DatasetType.SV,
            ),
            'seqr-datasets/GRCh37/v03/SV/lookup.ht',
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

    def test_remapped_and_subsetted_callset_path(self) -> None:
        self.assertEqual(
            remapped_and_subsetted_callset_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
                'gs://abc.efg/callset.vcf.gz',
                'gs://abc.efg/projects/R0111_tgg_bblanken_wes/pedigree.tsv',
            ),
            'gs://seqr-loading-temp/GRCh38/v03/GCNV/remapped_and_subsetted_callsets/1fb4a23aa1e331454e4d3055b77c4639bbb36d1dce462960cb542dec0ee51e87.mt',
        )
        self.assertEqual(
            remapped_and_subsetted_callset_path(
                Env.DEV,
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
                'gs://abc.efg/callset/*.vcf.gz',
                'gs://abc.efg/projects/R0111_tgg_bblanken_wes/pedigree.tsv',
            ),
            'gs://seqr-scratch-temp/GRCh38/v03/GCNV/remapped_and_subsetted_callsets/333b20b1661de1f475c6b9d13f86fa08bcc6ac7ec637fe5212bca99dd6f6a25c.mt',
        )
