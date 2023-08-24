import unittest

from v03_pipeline.lib.model import (
    DatasetType,
    Env,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import (
    family_table_path,
    imported_callset_path,
    metadata_for_run_path,
    project_table_path,
    remapped_and_subsetted_callset_path,
    sample_lookup_table_path,
    valid_reference_dataset_collection_path,
    variant_annotations_table_path,
)


class TestPaths(unittest.TestCase):
    def test_family_table_path(self) -> None:
        for env, expected_path in [
            (Env.TEST, 'seqr-datasets/v03/GRCh37/SNV_INDEL/families/franklin.ht'),
            (Env.LOCAL, 'seqr-datasets/v03/GRCh37/SNV_INDEL/families/franklin.ht'),
            (
                Env.DEV,
                'gs://seqr-scratch-temp/v03/GRCh37/SNV_INDEL/families/franklin.ht',
            ),
            (
                Env.PROD,
                'gs://seqr-datasets/v03/GRCh37/SNV_INDEL/families/franklin.ht',
            ),
        ]:
            self.assertEqual(
                family_table_path(
                    env,
                    ReferenceGenome.GRCh37,
                    DatasetType.SNV_INDEL,
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
            'gs://seqr-datasets/v03/GRCh38/MITO/projects/R0652_pipeline_test.ht',
        )

    def test_valid_reference_dataset_collection_path(self) -> None:
        self.assertEqual(
            valid_reference_dataset_collection_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                ReferenceDatasetCollection.HGMD,
            ),
            'gs://seqr-reference-data-private/v03/GRCh38/reference_datasets/hgmd.ht',
        )
        self.assertEqual(
            valid_reference_dataset_collection_path(
                Env.DEV,
                ReferenceGenome.GRCh37,
                ReferenceDatasetCollection.COMBINED,
            ),
            'gs://seqr-scratch-temp/v03/GRCh37/reference_datasets/combined.ht',
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
            'seqr-datasets/v03/GRCh37/SV/lookup.ht',
        )

    def test_metadata_for_run_path(self) -> None:
        self.assertEqual(
            metadata_for_run_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                'manual__2023-06-26T18:30:09.349671+00:00',
            ),
            'gs://seqr-datasets/v03/GRCh38/SNV_INDEL/runs/manual__2023-06-26T18:30:09.349671+00:00/metadata.json',
        )

    def test_variant_annotations_table_path(self) -> None:
        self.assertEqual(
            variant_annotations_table_path(
                Env.DEV,
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
            ),
            'gs://seqr-scratch-temp/v03/GRCh38/GCNV/annotations.ht',
        )

    def test_remapped_and_subsetted_callset_path(self) -> None:
        self.assertEqual(
            remapped_and_subsetted_callset_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
                'gs://abc.efg/callset.vcf.gz',
                'R0111_tgg_bblanken_wes',
            ),
            'gs://seqr-loading-temp/v03/GRCh38/GCNV/remapped_and_subsetted_callsets/902071e8f57f05930caa3e6c6c88010b6fbc9fe45de8dd17045133a269a2ee51.mt',
        )
        self.assertEqual(
            remapped_and_subsetted_callset_path(
                Env.DEV,
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
                'gs://abc.efg/callset/*.vcf.gz',
                'R0111_tgg_bblanken_wes',
            ),
            'gs://seqr-scratch-temp/v03/GRCh38/GCNV/remapped_and_subsetted_callsets/920f18219b92beb5c4506fbb6d4b6457770c2a756af691e277545b86990af457.mt',
        )

    def test_imported_callset_path(self) -> None:
        self.assertEqual(
            imported_callset_path(
                Env.PROD,
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                'gs://abc.efg/callset.vcf.gz',
            ),
            'gs://seqr-loading-temp/v03/GRCh38/SNV_INDEL/imported_callsets/ead56bb177a5de24178e1e622ce1d8beb3f8892bdae1c925d22ca0af4013d6dd.mt',
        )
