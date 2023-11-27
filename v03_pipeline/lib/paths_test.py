import unittest
from unittest.mock import patch

from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import (
    family_table_path,
    imported_callset_path,
    metadata_for_run_path,
    project_table_path,
    relatedness_check_table_path,
    remapped_and_subsetted_callset_path,
    sample_lookup_table_path,
    sex_check_table_path,
    valid_cached_reference_dataset_query_path,
    valid_reference_dataset_collection_path,
    variant_annotations_table_path,
)


class TestPaths(unittest.TestCase):
    def test_cached_reference_dataset_query_path(self) -> None:
        self.assertEqual(
            valid_cached_reference_dataset_query_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS,
            ),
            '/seqr-reference-data/v03/GRCh38/SNV_INDEL/cached_reference_dataset_queries/clinvar_path_variants.ht',
        )

    def test_family_table_path(self) -> None:
        self.assertEqual(
            family_table_path(
                ReferenceGenome.GRCh37,
                DatasetType.SNV_INDEL,
                'franklin',
            ),
            '/hail-search-data/v03/GRCh37/SNV_INDEL/families/franklin.ht',
        )
        with patch('v03_pipeline.lib.paths.Env') as mock_env:
            mock_env.DATASETS = 'gs://seqr-datasets/'
            self.assertEqual(
                family_table_path(
                    ReferenceGenome.GRCh37,
                    DatasetType.SNV_INDEL,
                    'franklin',
                ),
                'gs://seqr-datasets/v03/GRCh37/SNV_INDEL/families/franklin.ht',
            )

    def test_project_table_path(self) -> None:
        self.assertEqual(
            project_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                'R0652_pipeline_test',
            ),
            '/hail-search-data/v03/GRCh38/MITO/projects/R0652_pipeline_test.ht',
        )

    def test_valid_reference_dataset_collection_path(self) -> None:
        with patch('v03_pipeline.lib.paths.Env') as mock_env:
            mock_env.ACCESS_PRIVATE_DATASETS = False
            self.assertEqual(
                valid_reference_dataset_collection_path(
                    ReferenceGenome.GRCh37,
                    DatasetType.SNV_INDEL,
                    ReferenceDatasetCollection.HGMD,
                ),
                None,
            )
        self.assertEqual(
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.HGMD,
            ),
            '/seqr-reference-data-private/v03/GRCh38/SNV_INDEL/reference_datasets/hgmd.ht',
        )

    def test_sample_lookup_table_path(self) -> None:
        self.assertEqual(
            sample_lookup_table_path(
                ReferenceGenome.GRCh37,
                DatasetType.SV,
            ),
            '/hail-search-data/v03/GRCh37/SV/lookup.ht',
        )

    def test_sex_check_table_path(self) -> None:
        self.assertEqual(
            sex_check_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                'gs://abc.efg/callset.vcf.gz',
            ),
            '/seqr-loading-temp/v03/GRCh38/SNV_INDEL/sex_check/ead56bb177a5de24178e1e622ce1d8beb3f8892bdae1c925d22ca0af4013d6dd.ht',
        )

    def test_relatedness_check_table_path(self) -> None:
        self.assertEqual(
            relatedness_check_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                'gs://abc.efg/callset.vcf.gz',
            ),
            '/seqr-loading-temp/v03/GRCh38/SNV_INDEL/relatedness_check/ead56bb177a5de24178e1e622ce1d8beb3f8892bdae1c925d22ca0af4013d6dd.ht',
        )

    def test_metadata_for_run_path(self) -> None:
        self.assertEqual(
            metadata_for_run_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                'manual__2023-06-26T18:30:09.349671+00:00',
            ),
            '/hail-search-data/v03/GRCh38/SNV_INDEL/runs/manual__2023-06-26T18:30:09.349671+00:00/metadata.json',
        )

    def test_variant_annotations_table_path(self) -> None:
        self.assertEqual(
            variant_annotations_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
            ),
            '/hail-search-data/v03/GRCh38/GCNV/annotations.ht',
        )

    def test_remapped_and_subsetted_callset_path(self) -> None:
        self.assertEqual(
            remapped_and_subsetted_callset_path(
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
                'gs://abc.efg/callset.vcf.gz',
                'R0111_tgg_bblanken_wes',
            ),
            '/seqr-loading-temp/v03/GRCh38/GCNV/remapped_and_subsetted_callsets/R0111_tgg_bblanken_wes/ead56bb177a5de24178e1e622ce1d8beb3f8892bdae1c925d22ca0af4013d6dd.mt',
        )
        self.assertEqual(
            remapped_and_subsetted_callset_path(
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
                'gs://abc.efg/callset/*.vcf.gz',
                'R0111_tgg_bblanken_wes',
            ),
            '/seqr-loading-temp/v03/GRCh38/GCNV/remapped_and_subsetted_callsets/R0111_tgg_bblanken_wes/bce53ccdb49a5ed2513044e1d0c6224e3ffcc323f770dc807d9175fd3c70a050.mt',
        )

    def test_imported_callset_path(self) -> None:
        self.assertEqual(
            imported_callset_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                'gs://abc.efg/callset.vcf.gz',
            ),
            '/seqr-loading-temp/v03/GRCh38/SNV_INDEL/imported_callsets/ead56bb177a5de24178e1e622ce1d8beb3f8892bdae1c925d22ca0af4013d6dd.mt',
        )
